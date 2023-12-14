/*
Shamelessly borrowed from apps/httpd
 */


#include <seastar/http/httpd.hh>
#include <seastar/http/handlers.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/file_handler.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/reactor.hh>
#include <seastar/http/api_docs.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/print.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/tmp_file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/coroutine.hh>

#include <fmt/core.h>
#include <string_view>
// stop_signal.* is part of apps, not installed:
#include "../seastar/apps/lib/stop_signal.hh"


namespace bpo = boost::program_options;

using namespace seastar;
using namespace httpd;

using std::unique_ptr;
//TODO: adjust httpd_test to test & debug


class t_mru_cache {
    struct item {
        sstring data;
        long    priority;
        bool operator<( const item& other) const { return this->priority < other.priority;}
        item(sstring&& dat, int pri) : data(std::move(dat)), priority(pri) {}
    };
    std::map<sstring, item> cache;
    int capacity = 1000;  // TODO: take from app config

    void reset_pri() {
        long cur_pri = LONG_MAX - capacity;
        for (auto& [key, value]: cache)
                value.priority = cur_pri++;
    }
public:    
    void update(const sstring& key, sstring&& value) {
        long new_pri = cache.size() == 0 ?
                        LONG_MAX : cache.begin()->second.priority - 1;
        if(new_pri == LONG_MIN)
            reset_pri(); // needed only for veeeeery long running apps

        // Alternate design: compare hashes of prev/cur values to avoid redundant 
        // erase+emplace, but for now this is the easiest way to maintain MRU order:
        auto it = cache.find(key);
        if(it != cache.end()) {
            cache.erase(it);
        }
        if(cache.size() == capacity)
            cache.erase(cache.rbegin()->first); // pop oldest
        
        cache.emplace(std::pair(key, item(std::move(value), new_pri))); 
    }

    void erase(const sstring& key) {
        auto it = cache.find(key);
        if(it != cache.end())
            cache.erase(it);
    }

    const sstring* get(const sstring& key) {
        auto it = cache.find(key);
        return (it != cache.end()) ? &it->second.data : nullptr;
    }
};

seastar::sharded<t_mru_cache> sharded_cache; 

// Borrowed from seastar/demos/file_demo.cc:
constexpr size_t aligned_size = 4096;

static std::filesystem::path root_data_folder("/usr/seastar/dat");

unsigned int select_shard(const sstring& key) {
    // Aiming for balanced partition
    return std::hash<std::string_view>{}(std::string_view(key)) % seastar::smp::count;
}

class delete_handler : public handler_base {
public:
    virtual future<unique_ptr<http::reply> > handle(const sstring& path,
            unique_ptr<http::request> req, unique_ptr<http::reply> rep) {
        req->parse_query_param();
        verify_param(*req, "key");
        const auto& key = req->query_parameters["key"];
                            
        const sstring& filename = (root_data_folder / key.c_str()).native();
        co_await remove_file(filename); // no sync_directory - 
                                        // until the flush to disk, incoming `query` 
                                        // might return old contents.
        auto shard_idx = select_shard(key);
        sharded_cache.invoke_on(shard_idx, 
                [key] (t_mru_cache& local_cache) {
                    local_cache.erase(key);
                }).get(); 

        rep->done("json");
        co_return co_await make_ready_future<unique_ptr<http::reply>>(std::move(rep));
    }
};


class insert_handler : public handler_base {
public:
    virtual future<unique_ptr<http::reply> > handle(const sstring& path,
            unique_ptr<http::request> req, unique_ptr<http::reply> rep) {
        req->parse_query_param();
        verify_param(*req, "key");
        verify_param(*req, "value");

        const sstring& key = req->query_parameters["key"];
        auto shard_idx = select_shard(key);
        sharded_cache.invoke_on(shard_idx, 
                [req = std::move(req), rep = std::move(rep)] (t_mru_cache& local_cache)  {

                    const sstring& key = req->query_parameters["key"];
                    sstring& value = req->query_parameters["value"];
                    {
                        // temp debugging - log the insert. TODO make thread safe
                        const sstring val_prefix(value.c_str(), 20);
                        const sstring key_prefix(key.c_str(), 20);
                        fmt::print("  writing \"{}...\" into {}\n", val_prefix, key_prefix);
                    }
                    local_cache.update(key, std::move(value));

                    // The dma write is in multiples of 4K bytes, so we store the real value size in the first
                    // sizeof(size_t) bytes. The alignment padding is junk.
                    size_t raw_size = sizeof(size_t) + value.size();
                    size_t aligned_val_size = raw_size & 0xfffff000 +
                                            ((raw_size & 0xfff) != 0) * 0x1000;
                    auto wbuf = temporary_buffer<char>::aligned(aligned_size, aligned_val_size);
                    *(size_t*)wbuf.get_write() = value.size();

                    // TODO: async memcpy?
                    // Can we avoid the copy altogether by storing the req parameters on aligned boundaries in the first place?
                    memcpy(wbuf.get_write() + sizeof(size_t), value.c_str(), value.size());

                    const sstring& filename = (root_data_folder / key.c_str()).native();
                    auto f = seastar::open_file_dma(filename, open_flags::wo | open_flags::create).get();
                    auto count = f.dma_write(0, wbuf.get(), aligned_val_size).get();
                    assert(count == aligned_val_size);
                    
                    rep->_content = "Success";
                    rep->done("json"); // html?
                }
        ).get(); // invoke_on
        rep->done("json");
        co_return co_await make_ready_future<unique_ptr<http::reply>>(std::move(rep));
    }
};


class query_handler : public handler_base {
public:
    virtual future<unique_ptr<http::reply> > handle(const sstring& path,
            unique_ptr<http::request> req, unique_ptr<http::reply> rep) {
        req->parse_query_param();
        verify_param(*req, "key");
        const sstring& key = req->query_parameters["key"];
        auto shard_idx = select_shard(key);
        return sharded_cache.invoke_on(shard_idx, 
                seastar::coroutine::lambda([req = std::move(req), rep = std::move(rep)] (t_mru_cache& local_cache) 
                -> future<unique_ptr<http::reply> > {
                    
                    const sstring& key = req->query_parameters["key"];
                    const sstring* cached_val = local_cache.get(key);
                    if(cached_val) {
                        rep->write_body("json", [key, cached_val=std::move(cached_val)] (output_stream<char>&& os) mutable {
                                // TODO: write a formatter::write specialization for sstring pair
                                sstring res_line = "\"" + key + "\" : \"" + *cached_val + "\";";
                                return json::stream_object(res_line)(std::move(os)); });
                    }
                    else {
                        const sstring& filename = (root_data_folder / key.c_str()).native();
                        auto f = co_await open_file_dma(filename, open_flags::ro);
                        auto rbuf = allocate_aligned_buffer<unsigned char>(aligned_size, aligned_size);
                        auto rb = rbuf.get();
                        size_t read_bytes = co_await f.dma_read(0, rb, aligned_size);
                        size_t val_size = *(reinterpret_cast<size_t*>(rb));
                        
                        rep->write_body("json", [key, rb=std::move(rb), val_size] (output_stream<char>&& os) mutable {
                                // TODO: write a formatter::write specialization for sstring pair
                                sstring val(reinterpret_cast<char*>(rb + sizeof(size_t)), val_size);  // Oy vey
                                sstring res_line = "\"" + key + "\" : \"" + val + "\";";
                                return json::stream_object(res_line)(std::move(os)); });
                    }

                    rep->done("json");
                    co_return co_await make_ready_future<unique_ptr<http::reply> >(std::move(rep));
                }
            ) // seastar::coroutine::lambda
        ); // invoke_on
    }
};


class queryall_handler : public handler_base {
public:
    virtual future<unique_ptr<http::reply> > handle(const sstring& path,
            unique_ptr<http::request> req, unique_ptr<http::reply> rep) {
        rep->write_body("json", [req=std::move(req)] (output_stream<char>&& os) mutable {

        // TODO: implement with generator, keep_doing / yield per filename

        // for (auto const& dir_entry : std::filesystem::recursive_directory_iterator{root_data_folder}) {
        //     if(std::filesystem::is_directory(dir_entry)) // in preparation of fanout
        //         continue;

        //     // // with fanout this would require concatenation with parent folders
        //     // const auto& key = dir_entry.path().stem().string();
        //     // auto shard_idx = select_shard(key);
        //     // sharded_cache.invoke_on(shard_idx, 
        //     //     seastar::coroutine::lambda([req=std::move(req), rep=std::move(rep), os=std::move(os)] (t_mru_cache& local_cache) 
        //     //     -> future<unique_ptr<http::reply> >  {
                    
        //     //         const sstring& key = req->query_parameters["key"];
        //     //         const auto* cached_val = local_cache.get(key);

        //     //     }
        //     //     ) // seastar::coroutine::lambda
        //     // ); // invoke_on
        // } // recursive_directory_iterator
            return json::stream_object(sstring(""))(std::move(os));
        } //write_body lambda
        ); // write_body
// TODO        
        rep->done("json");
        co_return co_await make_ready_future<unique_ptr<http::reply>>(std::move(rep));
    }
};

void set_routes(routes& r) {

// TODO: file compression?
// TODO: try git.objects-like directory fanout based on first byte of hash
// Not sure whether both are preferrable in typical seastar workloads. We might prefer
// longer IO over longer compute (IO is overlapped).

// TODO: in /apps/httpd/main.cc handlers aren't freed. Leak?
    auto* ih = new insert_handler();
    auto* qah = new queryall_handler();
    auto* qh = new query_handler();
    auto* dh = new delete_handler();

    r.add(operation_type::PUT, url("/insert"), ih);   // handles also update 
    r.add(operation_type::GET, url("/queryall"), qah);
    r.add(operation_type::GET, url("/query"), qh);
    r.add(operation_type::PUT, url("/delete"), dh);

}

int main(int ac, char** av) {
    http_server_control prometheus_server;
    app_template app;

    app.add_options()("port", bpo::value<uint16_t>()->default_value(10000), "HTTP Server port");
    app.add_options()("cache_size", bpo::value<uint16_t>()->default_value(1000), "Entry cache size");

    return app.run(ac, av, [&] {
        return seastar::async([&] {
            seastar_apps_lib::stop_signal stop_signal;
            auto&& config = app.configuration();
            uint16_t port = config["port"].as<uint16_t>();
            auto server = new http_server_control();
            server->start().get();

            auto stop_server = defer([&] () noexcept {
                std::cout << "Stoppping HTTP server" << std::endl; // This can throw, but won't.
                server->stop().get();
            });

            server->set_routes(set_routes).get();
            server->listen(port).get();

            std::cout << "Seastar HTTP server listening on port " << port << " ...\n";

            return 0;
        });
    });
}

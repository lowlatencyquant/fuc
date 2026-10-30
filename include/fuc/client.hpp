#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace fuc {
using Fields = std::vector<std::pair<std::string, std::string>>;
struct Config {
  std::string api_url = "http://127.0.0.1:8081";
  std::string keycloak_url = "http://127.0.0.1:8082";
  std::string realm;
  std::string client_id;
  std::string client_secret;
  std::string username;
  std::string password;
  std::string scope = "openid";
  std::string ca_bundle;
  long timeout_seconds = 30;
  long connect_timeout_seconds = 5;
  bool verify_tls = true;
  static Config Load(const std::string& path);
};
struct Request {
  std::string method;
  std::string path;
  Fields query;
  Fields headers;
  std::string body;
  bool authenticated = true;
};
struct Response {
  long status;
  std::string body;
};
std::string Encode(std::string_view value);
class Http {
 public:
  explicit Http(Config config);
  Response Send(const std::string& url, const std::string& method,
                const Fields& headers, const std::string& body) const;

 private:
  Config config_;
};
class Client {
 public:
  explicit Client(Config config);
  void Login();
  Response Execute(const Request& request);

 private:
  Config config_;
  Http http_;
  std::string token_;
};
}

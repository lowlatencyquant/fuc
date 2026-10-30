#include "fuc/client.hpp"

#include <curl/curl.h>
#include <yaml-cpp/yaml.h>

#include <cstdlib>
#include <memory>
#include <nlohmann/json.hpp>
#include <stdexcept>

namespace fuc {
namespace {
struct CurlGlobal {
  CurlGlobal() {
    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK)
      throw std::runtime_error("Cannot initialize libcurl");
  }
  ~CurlGlobal() { curl_global_cleanup(); }
};
std::size_t Write(char* data, std::size_t size, std::size_t count,
                  void* target) {
  auto& output = *static_cast<std::string*>(target);
  auto bytes = size * count;
  if (bytes > 64 * 1024 * 1024 - output.size()) return 0;
  try {
    output.append(data, bytes);
  } catch (...) {
    return 0;
  }
  return bytes;
}
std::string Join(std::string base, const std::string& path) {
  while (!base.empty() && base.back() == '/') base.pop_back();
  if (!(base.starts_with("http://") || base.starts_with("https://")))
    throw std::runtime_error("Base URL must use http:// or https://");
  return base + path;
}
std::string Form(const Fields& fields) {
  std::string result;
  for (const auto& [key, value] : fields) {
    if (!result.empty()) result += '&';
    result += Encode(key) + '=' + Encode(value);
  }
  return result;
}
}
std::string Encode(std::string_view value) {
  constexpr char hex[] = "0123456789ABCDEF";
  std::string output;
  for (unsigned char c : value) {
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' ||
        c == '~') {
      output += static_cast<char>(c);
    } else {
      output += '%';
      output += hex[c >> 4];
      output += hex[c & 15];
    }
  }
  return output;
}
Config Config::Load(const std::string& path) {
  Config config;
  YAML::Node node =
      path.empty() ? YAML::Node(YAML::NodeType::Map) : YAML::LoadFile(path);
  auto string = [&]<typename T>(const char* key, const char* env, T& value) {
    if (node[key]) value = node[key].as<T>();
    if (const char* override_value = std::getenv(env))
      value = YAML::Node(override_value).as<T>();
  };
  string("api_url", "FUC_API_URL", config.api_url);
  string("keycloak_url", "FUC_KEYCLOAK_URL", config.keycloak_url);
  string("realm", "FUC_REALM", config.realm);
  string("client_id", "FUC_CLIENT_ID", config.client_id);
  string("client_secret", "FUC_CLIENT_SECRET", config.client_secret);
  string("username", "FUC_USERNAME", config.username);
  string("password", "FUC_PASSWORD", config.password);
  string("scope", "FUC_SCOPE", config.scope);
  string("ca_bundle", "FUC_CA_BUNDLE", config.ca_bundle);
  string("timeout_seconds", "FUC_TIMEOUT_SECONDS", config.timeout_seconds);
  string("connect_timeout_seconds", "FUC_CONNECT_TIMEOUT_SECONDS",
         config.connect_timeout_seconds);
  string("verify_tls", "FUC_VERIFY_TLS", config.verify_tls);
  if (config.timeout_seconds <= 0 || config.connect_timeout_seconds <= 0)
    throw std::runtime_error("Timeouts must be positive");
  return config;
}
Http::Http(Config config) : config_(std::move(config)) {}
Response Http::Send(const std::string& url, const std::string& method,
                    const Fields& headers, const std::string& body) const {
  static CurlGlobal global;
  std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> handle(curl_easy_init(),
                                                             curl_easy_cleanup);
  if (!handle) throw std::runtime_error("Cannot create HTTP handle");
  std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> list(
      nullptr, curl_slist_free_all);
  for (const auto& [key, value] : headers) {
    if (key.find_first_of("\r\n:") != std::string::npos ||
        value.find_first_of("\r\n") != std::string::npos)
      throw std::runtime_error("Invalid HTTP header");
    auto* next = curl_slist_append(list.get(), (key + ": " + value).c_str());
    if (!next) throw std::bad_alloc();
    list.release();
    list.reset(next);
  }
  Response response{};
  char error[CURL_ERROR_SIZE]{};
  auto set = [&]<typename T>(CURLoption option, T value) {
    if (curl_easy_setopt(handle.get(), option, value) != CURLE_OK)
      throw std::runtime_error("Cannot configure HTTP request");
  };
  set(CURLOPT_URL, url.c_str());
  set(CURLOPT_PROTOCOLS_STR, "http,https");
  set(CURLOPT_CUSTOMREQUEST, method.c_str());
  set(CURLOPT_HTTPHEADER, list.get());
  set(CURLOPT_FOLLOWLOCATION, 0L);
  set(CURLOPT_NOSIGNAL, 1L);
  set(CURLOPT_TIMEOUT, config_.timeout_seconds);
  set(CURLOPT_CONNECTTIMEOUT, config_.connect_timeout_seconds);
  set(CURLOPT_SSL_VERIFYPEER, config_.verify_tls ? 1L : 0L);
  set(CURLOPT_SSL_VERIFYHOST, config_.verify_tls ? 2L : 0L);
  if (!config_.ca_bundle.empty())
    set(CURLOPT_CAINFO, config_.ca_bundle.c_str());
  set(CURLOPT_ERRORBUFFER, error);
  set(CURLOPT_WRITEFUNCTION, &Write);
  set(CURLOPT_WRITEDATA, &response.body);
  if (!body.empty()) {
    set(CURLOPT_POSTFIELDS, body.data());
    set(CURLOPT_POSTFIELDSIZE_LARGE, static_cast<curl_off_t>(body.size()));
  }
  auto status = curl_easy_perform(handle.get());
  if (status != CURLE_OK)
    throw std::runtime_error("HTTP transport failed: " + std::string(error));
  curl_easy_getinfo(handle.get(), CURLINFO_RESPONSE_CODE, &response.status);
  return response;
}
Client::Client(Config config) : config_(std::move(config)), http_(config_) {}
void Client::Login() {
  if (config_.realm.empty() || config_.client_id.empty() ||
      config_.username.empty() || config_.password.empty())
    throw std::runtime_error(
        "Login requires realm, client_id, username and password (config or "
        "FUC_* environment)");
  Fields form{{"grant_type", "password"},
              {"client_id", config_.client_id},
              {"username", config_.username},
              {"password", config_.password},
              {"scope", config_.scope}};
  if (!config_.client_secret.empty())
    form.emplace_back("client_secret", config_.client_secret);
  auto response = http_.Send(
      Join(config_.keycloak_url, "/realms/" + Encode(config_.realm) +
                                     "/protocol/openid-connect/token"),
      "POST", {{"Content-Type", "application/x-www-form-urlencoded"}},
      Form(form));
  if (response.status != 200)
    throw std::runtime_error(
        "Keycloak login failed (HTTP " + std::to_string(response.status) +
        "). Check credentials, realm, client and Direct Access Grants.");
  auto json = nlohmann::json::parse(response.body);
  token_ = json.at("access_token").get<std::string>();
  if (token_.empty())
    throw std::runtime_error("Keycloak returned an empty access token");
}
Response Client::Execute(const Request& request) {
  auto headers = request.headers;
  if (request.authenticated) {
    if (token_.empty()) Login();
    headers.emplace_back("Authorization", "Bearer " + token_);
  }
  auto url = Join(config_.api_url, request.path);
  if (!request.query.empty()) url += '?' + Form(request.query);
  auto response = http_.Send(url, request.method, headers, request.body);
  if (response.status < 200 || response.status >= 300)
    throw std::runtime_error("API returned HTTP " +
                             std::to_string(response.status) + ": " +
                             response.body);
  return response;
}
}

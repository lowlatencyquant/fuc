#pragma once

#include <array>
#include <charconv>
#include <concepts>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "fuc/client.hpp"

namespace fuc {
template <std::size_t N>
struct FixedString {
  char value[N];
  constexpr FixedString(const char (&text)[N]) {
    for (std::size_t i = 0; i < N; ++i) value[i] = text[i];
  }
  constexpr operator std::string_view() const { return {value, N - 1}; }
};
template <FixedString Method, FixedString Path, bool Authenticated = true>
struct Endpoint {
  static Request Make() {
    return {std::string(std::string_view(Method)),
            std::string(std::string_view(Path)),
            {},
            {{"Accept", "application/json"}},
            "",
            Authenticated};
  }
};
template <typename T>
struct Option {
  std::string_view name;
  std::string_view description;
  bool required;
  bool repeated;
  void (*assign)(T&, std::string_view);
};
template <typename T>
struct MemberType;
template <typename T, typename V>
struct MemberType<V T::*> {
  using owner = T;
  using value = V;
};
template <auto Member>
constexpr auto Bind(std::string_view name, std::string_view description,
                    bool required = false) {
  using Owner = typename MemberType<decltype(Member)>::owner;
  using Value = typename MemberType<decltype(Member)>::value;
  return Option<Owner>{
      name, description, required,
      std::same_as<Value, std::vector<std::string>>,
      [](Owner& object, std::string_view text) {
        if (text.empty()) throw std::runtime_error("Value must not be empty");
        if constexpr (std::same_as<Value, std::string>) {
          object.*Member = text;
        } else if constexpr (std::same_as<Value, std::vector<std::string>>) {
          (object.*Member).emplace_back(text);
        } else if constexpr (std::same_as<Value, std::optional<bool>>) {
          if (text != "true" && text != "false")
            throw std::runtime_error("Expected true or false");
          object.*Member = text == "true";
        } else {
          std::int64_t value;
          auto [end, error] =
              std::from_chars(text.data(), text.data() + text.size(), value);
          if (error != std::errc{} || end != text.data() + text.size())
            throw std::runtime_error("Expected a whole number");
          object.*Member = value;
        }
      }};
}
struct Login {
  static constexpr std::string_view usage = "login";
  static constexpr std::string_view description =
      "Test Keycloak credentials. Protected commands log in automatically.";
  static constexpr std::array<Option<Login>, 0> options{};
  void Validate() const {}
};
struct Health : Endpoint<"GET", "/health", false> {
  static constexpr std::string_view usage = "health";
  static constexpr std::string_view description =
      "Check API health without login or other resources.";
  static constexpr std::array<Option<Health>, 0> options{};
  void Validate() const {}
  Request Build() const;
};
struct Artifacts : Endpoint<"GET", "/api/index/api/v1/artifacts"> {
  std::int64_t page = 0;
  std::int64_t page_size = 20;
  std::string name;
  std::string tag;
  std::vector<std::string> types;
  std::string sort;
  std::string order;
  std::optional<std::int64_t> min_versions;
  std::optional<std::int64_t> max_versions;
  std::optional<bool> has_files;
  static constexpr std::string_view usage = "artifacts [OPTIONS]";
  static constexpr std::string_view description =
      "Read artifacts with named filters; no prior CLI command is needed.";
  static constexpr auto options = std::array{
      Bind<&Artifacts::page>("--page", "Zero-based page (default: 0)"),
      Bind<&Artifacts::page_size>("--page-size",
                                  "Items per page (default: 20)"),
      Bind<&Artifacts::name>("--name", "Full-name prefix"),
      Bind<&Artifacts::tag>("--tag", "Filter by assigned tag"),
      Bind<&Artifacts::types>("--type",
                              "Artifact type; repeat for OR matching"),
      Bind<&Artifacts::sort>(
          "--sort", "createdAt, name, versionCount, largestVersion, totalSize"),
      Bind<&Artifacts::order>("--order", "asc or desc"),
      Bind<&Artifacts::min_versions>("--min-versions", "Minimum version count"),
      Bind<&Artifacts::max_versions>("--max-versions", "Maximum version count"),
      Bind<&Artifacts::has_files>("--has-files", "true or false")};
  void Validate() const;
  Request Build() const;
};
struct CreateKafka : Endpoint<"POST", "/api/weave/api/v1/kafkatriggers"> {
  std::string name;
  std::string chain;
  std::vector<std::string> brokers;
  std::string topic;
  std::string group;
  std::string secret;
  std::vector<std::string> events;
  std::vector<std::string> buckets;
  std::optional<std::int64_t> max_concurrent;
  static constexpr std::string_view usage =
      "kafka create NAME --chain NAME --broker HOST:PORT --topic TOPIC --group "
      "GROUP [OPTIONS]";
  static constexpr std::string_view description =
      "Create a Kafka trigger. Builds the nested JSON and headers "
      "automatically. The referenced chain must exist.";
  static constexpr auto options = std::array{
      Bind<&CreateKafka::chain>("--chain", "Existing chain name", true),
      Bind<&CreateKafka::brokers>(
          "--broker", "Broker address; repeat for multiple brokers", true),
      Bind<&CreateKafka::topic>("--topic", "Kafka topic", true),
      Bind<&CreateKafka::group>("--group", "Kafka consumer group", true),
      Bind<&CreateKafka::secret>("--secret",
                                 "Existing Kafka credentials secret name"),
      Bind<&CreateKafka::events>(
          "--event", "put, delete or get; repeat to select multiple"),
      Bind<&CreateKafka::buckets>("--bucket",
                                  "Bucket filter; repeat to select multiple"),
      Bind<&CreateKafka::max_concurrent>("--max-concurrent",
                                         "Concurrent runs; 0 means unlimited")};
  void Validate() const;
  Request Build() const;
};
struct DeleteKafka : Endpoint<"DELETE", "/api/weave/api/v1/kafkatriggers/"> {
  std::string name;
  static constexpr std::string_view usage = "kafka delete NAME";
  static constexpr std::string_view description =
      "Delete the named Kafka trigger. No request body or prior CLI login is "
      "needed.";
  static constexpr std::array<Option<DeleteKafka>, 0> options{};
  void Validate() const;
  Request Build() const;
};
template <typename T>
concept ApiCommand = requires(const T& command) {
  { command.Build() } -> std::same_as<Request>;
};
}

#include "fuc/commands.hpp"

#include <algorithm>
#include <initializer_list>
#include <nlohmann/json.hpp>

namespace fuc {
namespace {
void Choice(std::string_view option, std::string_view value,
            std::initializer_list<std::string_view> allowed) {
  if (std::ranges::find(allowed, value) == allowed.end())
    throw std::runtime_error(std::string(option) + ": unsupported value '" +
                             std::string(value) + "'");
}
void Nonnegative(std::string_view option, std::int64_t value) {
  if (value < 0)
    throw std::runtime_error(std::string(option) + " must be zero or greater");
}
void Name(std::string_view value) {
  if (value.empty() || value == "." || value == ".." ||
      std::ranges::any_of(value,
                          [](unsigned char c) { return c <= 32 || c == 127; }))
    throw std::runtime_error(
        "NAME must be nonempty and contain no whitespace or control "
        "characters");
}
}
Request Health::Build() const { return Make(); }
void Artifacts::Validate() const {
  Nonnegative("--page", page);
  if (page_size <= 0)
    throw std::runtime_error("--page-size must be greater than zero");
  if (min_versions) Nonnegative("--min-versions", *min_versions);
  if (max_versions) Nonnegative("--max-versions", *max_versions);
  if (min_versions && max_versions && *min_versions > *max_versions)
    throw std::runtime_error("--min-versions must not exceed --max-versions");
  if (!sort.empty())
    Choice(
        "--sort", sort,
        {"createdAt", "name", "versionCount", "largestVersion", "totalSize"});
  if (!order.empty()) Choice("--order", order, {"asc", "desc"});
}
Request Artifacts::Build() const {
  auto request = Make();
  request.query = {{"page", std::to_string(page)},
                   {"pageSize", std::to_string(page_size)}};
  auto add = [&](const std::string& key, const std::string& value) {
    if (!value.empty()) request.query.emplace_back(key, value);
  };
  add("name", name);
  add("tag", tag);
  for (const auto& type : types) add("type", type);
  add("sort", sort);
  add("order", order);
  if (min_versions) add("minVersions", std::to_string(*min_versions));
  if (max_versions) add("maxVersions", std::to_string(*max_versions));
  if (has_files) add("hasFiles", *has_files ? "true" : "false");
  return request;
}
void CreateKafka::Validate() const {
  Name(name);
  if (chain.empty() || brokers.empty() || topic.empty() || group.empty())
    throw std::runtime_error(
        "Kafka creation requires --chain, --broker, --topic and --group");
  for (const auto& event : events)
    Choice("--event", event, {"put", "delete", "get"});
  if (max_concurrent) Nonnegative("--max-concurrent", *max_concurrent);
}
Request CreateKafka::Build() const {
  auto request = Make();
  nlohmann::json kafka{
      {"brokers", brokers}, {"topic", topic}, {"consumerGroup", group}};
  if (!secret.empty()) kafka["secretRef"] = {{"name", secret}};
  if (!events.empty()) kafka["eventFilter"] = events;
  if (!buckets.empty()) kafka["bucketFilter"] = buckets;
  if (max_concurrent) kafka["maxConcurrentRuns"] = *max_concurrent;
  request.body = nlohmann::json{{"name", name},
                                {"chainRef", {{"name", chain}}},
                                {"kafka", std::move(kafka)}}
                     .dump();
  request.headers.emplace_back("Content-Type", "application/json");
  return request;
}
void DeleteKafka::Validate() const { Name(name); }
Request DeleteKafka::Build() const {
  auto request = Make();
  request.path += Encode(name);
  return request;
}
}

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <nlohmann/json.hpp>
#include <set>
#include <span>
#include <stdexcept>

#include "fuc/commands.hpp"

namespace {
using Arguments = std::span<const std::string_view>;
struct Settings {
  std::string config;
  bool config_seen = false;
  bool dry_run = false;
};
void Help() {
  std::cout << R"(Usage: fuc [--config FILE] COMMAND

Commands:
  login                Test Keycloak credentials
  health               Check API health without authentication
  artifacts            Read artifacts using named filters
  kafka create NAME    Create a Kafka trigger using named options
  kafka delete NAME    Delete a Kafka trigger

Protected commands log in automatically; running login first is unnecessary.
Use COMMAND --help for options and examples.
Configuration: --config FILE or FUC_CONFIG, with FUC_* environment overrides.
API commands accept --dry-run to preview the request without login or network.
)";
}
void KafkaHelp() {
  std::cout << "Usage: fuc kafka create NAME [OPTIONS]\n"
               "       fuc kafka delete NAME\n"
               "Use 'fuc kafka create --help' for creation options.\n";
}
struct Argument {
  std::string_view key;
  std::optional<std::string_view> inline_value;
};
Argument Split(std::string_view argument) {
  const auto separator = argument.find('=');
  if (separator == std::string_view::npos) return {argument, std::nullopt};
  return {argument.substr(0, separator), argument.substr(separator + 1)};
}
std::string_view Value(const Argument& argument, Arguments args,
                       std::size_t& i) {
  if (argument.inline_value) return *argument.inline_value;
  if (i + 1 == args.size() || args[i + 1].starts_with("--") ||
      args[i + 1] == "-h")
    throw std::runtime_error("Missing value for " + std::string(argument.key));
  return args[++i];
}
void Config(const Argument& argument, Arguments args, std::size_t& i,
            Settings& settings) {
  if (settings.config_seen)
    throw std::runtime_error("--config may be supplied only once");
  auto value = Value(argument, args, i);
  if (value.empty()) throw std::runtime_error("--config requires a file path");
  settings.config = value;
  settings.config_seen = true;
}
template <typename Command>
void CommandHelp() {
  std::cout << "Usage: fuc [--config FILE] " << Command::usage << "\n\n"
            << Command::description << "\n\nOptions:\n";
  for (const auto& option : Command::options) {
    std::cout << "  " << option.name << " VALUE";
    if (option.required) std::cout << " (required)";
    if (option.repeated) std::cout << " (repeatable)";
    std::cout << "\n    " << option.description << '\n';
  }
  std::cout << "  --config FILE\n    Configuration file; environment variables "
               "override it\n"
               "  --help\n    Show command help\n";
  if constexpr (fuc::ApiCommand<Command>)
    std::cout << "  --dry-run\n    Show the constructed request without login "
                 "or network\n";
  if constexpr (std::same_as<Command, fuc::CreateKafka>) {
    std::cout << R"(
Example:
  fuc --config config.local.yaml kafka create orders \
    --chain process-orders --broker kafka:9092 \
    --topic orders --group orders-workers \
    --event put --bucket incoming --max-concurrent 2
)";
  } else if constexpr (std::same_as<Command, fuc::Artifacts>) {
    std::cout << "\nExample: fuc artifacts --name org.data --type dataset "
                 "--page-size 50\n";
  }
}
void Preview(const fuc::Request& request) {
  std::cout << request.method << ' ' << request.path;
  bool first = true;
  for (const auto& [key, value] : request.query) {
    std::cout << (first ? '?' : '&') << fuc::Encode(key) << '='
              << fuc::Encode(value);
    first = false;
  }
  std::cout << '\n';
  for (const auto& [key, value] : request.headers)
    std::cout << key << ": " << value << '\n';
  if (request.authenticated)
    std::cout
        << "Authorization: Bearer <obtained automatically at execution>\n";
  if (!request.body.empty())
    std::cout << '\n' << nlohmann::json::parse(request.body).dump(2) << '\n';
}
void Output(const fuc::Response& response) {
  if (response.body.empty()) {
    std::cout << "Success (HTTP " << response.status << ")\n";
    return;
  }
  auto json = nlohmann::json::parse(response.body, nullptr, false);
  if (!json.is_discarded())
    std::cout << json.dump(2) << '\n';
  else {
    std::cout << response.body;
    if (response.body.back() != '\n') std::cout << '\n';
  }
}
template <typename Command>
int Run(Arguments args, Settings settings) {
  Command command;
  std::set<std::string_view> seen;
  bool name_seen = false;
  for (std::size_t i = 0; i < args.size(); ++i) {
    const auto argument = Split(args[i]);
    if (args[i] == "--help" || args[i] == "-h") {
      CommandHelp<Command>();
      return 0;
    }
    if (argument.key == "--config") {
      Config(argument, args, i, settings);
    } else if (args[i] == "--dry-run" && fuc::ApiCommand<Command>) {
      if (settings.dry_run) throw std::runtime_error("Duplicate --dry-run");
      settings.dry_run = true;
    } else if (args[i].starts_with('-')) {
      const auto option = std::ranges::find(Command::options, argument.key,
                                            &fuc::Option<Command>::name);
      if (option == Command::options.end())
        throw std::runtime_error("Unknown option for this command: " +
                                 std::string(argument.key));
      if (!seen.insert(option->name).second && !option->repeated)
        throw std::runtime_error("Duplicate option: " +
                                 std::string(option->name));
      const auto value = Value(argument, args, i);
      try {
        option->assign(command, value);
      } catch (const std::exception& error) {
        throw std::runtime_error(std::string(option->name) + ": " +
                                 error.what());
      }
    } else {
      if constexpr (std::same_as<Command, fuc::CreateKafka> ||
                    std::same_as<Command, fuc::DeleteKafka>) {
        if (name_seen)
          throw std::runtime_error("Only one trigger NAME is allowed");
        command.name = args[i];
        name_seen = true;
      } else {
        throw std::runtime_error("Unexpected argument: " +
                                 std::string(args[i]));
      }
    }
  }
  for (const auto& option : Command::options)
    if (option.required && !seen.contains(option.name))
      throw std::runtime_error("Missing required option: " +
                               std::string(option.name));
  command.Validate();
  if constexpr (fuc::ApiCommand<Command>) {
    auto request = command.Build();
    if (settings.dry_run) {
      Preview(request);
      return 0;
    }
    fuc::Client client(fuc::Config::Load(settings.config));
    Output(client.Execute(request));
  } else {
    fuc::Client client(fuc::Config::Load(settings.config));
    client.Login();
    std::cout << "Keycloak login succeeded\n";
  }
  return 0;
}
}
int main(int argc, char** argv) {
  try {
    std::vector<std::string_view> arguments(argv + 1, argv + argc);
    Arguments args(arguments);
    Settings settings;
    if (const char* config = std::getenv("FUC_CONFIG"))
      settings.config = config;
    std::size_t i = 0;
    for (; i < args.size(); ++i) {
      if (args[i] == "--help" || args[i] == "-h") {
        Help();
        return 0;
      }
      auto argument = Split(args[i]);
      if (argument.key != "--config") break;
      Config(argument, args, i, settings);
    }
    if (i == args.size()) {
      Help();
      return 0;
    }
    const auto command = args[i++];
    if (command == "login") return Run<fuc::Login>(args.subspan(i), settings);
    if (command == "health") return Run<fuc::Health>(args.subspan(i), settings);
    if (command == "artifacts")
      return Run<fuc::Artifacts>(args.subspan(i), settings);
    if (command == "kafka") {
      if (i == args.size() || args[i] == "--help" || args[i] == "-h") {
        KafkaHelp();
        return 0;
      }
      const auto action = args[i++];
      if (action == "create")
        return Run<fuc::CreateKafka>(args.subspan(i), settings);
      if (action == "delete")
        return Run<fuc::DeleteKafka>(args.subspan(i), settings);
      throw std::runtime_error("Unknown kafka action: " + std::string(action));
    }
    throw std::runtime_error("Unknown command: " + std::string(command));
  } catch (const std::exception& error) {
    std::cerr << "fuc: " << error.what()
              << "\nUse 'fuc --help' or COMMAND --help for usage.\n";
    return 1;
  }
}

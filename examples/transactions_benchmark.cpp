#include <algorithm>
#include <chrono>
#include <couchbase/cluster.hxx>
#include <couchbase/codec/tao_json_serializer.hxx>
#include <couchbase/durability_level.hxx>
#include <couchbase/logger.hxx>
#include <couchbase/transactions/attempt_context.hxx>

#include <fmt/chrono.h>
#include <fmt/format.h>

#include <tao/json.hpp>
#include <tao/json/to_string.hpp>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <limits>
#include <random>

struct program_config {
  std::string connection_string{ "couchbase://127.0.0.1" };
  std::string user_name{ "Administrator" };
  std::string password{ "password" };
  std::string preferred_server_group{ "Group 1" };
  std::string bucket_name{ "default" };
  std::string scope_name{ couchbase::scope::default_name };
  std::string collection_name{ couchbase::collection::default_name };
  std::optional<std::string> profile{};
  bool verbose{ false };

  static auto from_env() -> program_config;
  static auto quote(std::string val) -> std::string;
  void dump();
};

namespace generator
{
auto
keys_for_vbuckets(const std::string& prefix,
                  std::size_t number_of_vbuckets,
                  std::size_t number_of_keys_per_vbucket) -> std::vector<std::string>;

auto
keys(const std::string& prefix, std::size_t number_of_keys) -> std::vector<std::string>;
auto
json_objects(std::size_t number_of_objects, std::size_t minimum_size_of_object)
  -> std::vector<tao::json::value>;

auto
encoded_json_objects(std::size_t number_of_objects, std::size_t minimum_size_of_object)
  -> std::vector<std::string>;

auto
encoded_csv_tables(std::size_t number_of_objects, std::size_t minimum_size_of_table)
  -> std::vector<std::string>;

template<typename Payload>
struct test_space {
  std::string description;
  std::vector<std::string> ids;
  std::vector<Payload> payloads;
};

auto
json_test_space(std::size_t number_of_objects, std::size_t minimum_size_of_object)
  -> test_space<tao::json::value>;
auto
encoded_json_test_space(std::size_t number_of_objects, std::size_t minimum_size_of_object)
  -> test_space<std::string>;
auto
encoded_csv_test_space(std::size_t number_of_objects, std::size_t minimum_size_of_object)
  -> test_space<std::string>;

} // namespace generator

template<typename Payload>
void
populate_test_space(const couchbase::collection& collection,
                    generator::test_space<Payload> test_space)
{
  auto upsert_options =
    couchbase::upsert_options{}.durability(couchbase::durability_level::majority);

  auto begin = std::chrono::high_resolution_clock::now();
  for (std::size_t i = 0; i < test_space.ids.size(); ++i) {
    auto [err, resp] =
      collection.upsert(test_space.ids[i], test_space.payloads[i], upsert_options).get();
    if (err.ec()) {
      throw std::runtime_error(
        fmt::format("unable to create document {}: {}", test_space.ids[i], err.message()));
    }
  }
  auto end = std::chrono::high_resolution_clock::now();

  fmt::println("{}: created in {} ({}/doc)",
               test_space.description,
               std::chrono::duration_cast<std::chrono::milliseconds>(end - begin),
               std::chrono::duration_cast<std::chrono::milliseconds>(end - begin) / test_space.ids.size());
}

int
main()
{
  auto config = program_config::from_env();
  config.dump();

  if (config.verbose) {
    couchbase::logger::initialize_console_logger();
    couchbase::logger::set_level(couchbase::logger::log_level::trace);
  }

  auto options = couchbase::cluster_options(config.user_name, config.password);
  options.network().preferred_server_group(config.preferred_server_group);
  if (config.profile) {
    options.apply_profile(config.profile.value());
  }

  auto [connect_err, cluster] =
    couchbase::cluster::connect(config.connection_string, options).get();
  if (connect_err) {
    std::cout << "Unable to connect to the cluster. ec: " << connect_err.message() << "\n";
    return EXIT_FAILURE;
  }

  auto collection =
    cluster.bucket(config.bucket_name).scope(config.scope_name).collection(config.collection_name);

  auto json_10k_1k = generator::json_test_space(10'000, 1'000);
  populate_test_space(collection, json_10k_1k);

  auto json_10k_10k = generator::json_test_space(10'000, 10'000);
  populate_test_space(collection, json_10k_10k);

  auto json_10k_100k = generator::json_test_space(10'000, 100'000);
  populate_test_space(collection, json_10k_100k);

  auto json_10k_1m = generator::json_test_space(10'000, 1'000'000);
  populate_test_space(collection, json_10k_1m);

#if 0
  {
    auto [err, res] = cluster.transactions()->run(
      [collection, preferred_server_group = config.preferred_server_group](
        std::shared_ptr<couchbase::transactions::attempt_context> ctx) -> couchbase::error {
        auto [e1, alice] = ctx->get_replica_from_preferred_server_group(collection, "alice");
        if (e1.ec()) {
          std::cout << "Unable to read account for Alice from preferred group \""
                    << preferred_server_group << "\": " << e1.ec().message()
                    << ". Falling back to regular get\n";
          auto [e2, alice_fallback] = ctx->get(collection, "alice");
          if (e2.ec()) {
            std::cout << "Unable to read account for Alice: " << e2.ec().message() << "\n";
            return e2;
          }
          alice = alice_fallback;
        }
        auto alice_content = alice.content_as<bank_account>();

        auto [e3, bob] = ctx->get_replica_from_preferred_server_group(collection, "bob");
        if (e3.ec()) {
          std::cout << "Unable to read account for Bob from preferred group \""
                    << preferred_server_group << "\": " << e3.ec().message()
                    << ". Falling back to regular get\n";
          auto [e4, bob_fallback] = ctx->get(collection, "bob");
          if (e4.ec()) {
            std::cout << "Unable to read account for Alice: " << e4.ec().message() << "\n";
            return e4;
          }
          bob = bob_fallback;
        }
        auto bob_content = bob.content_as<bank_account>();

        const std::int64_t money_to_transfer = 1'234;
        if (alice_content.balance < money_to_transfer) {
          std::cout << "Alice does not have enough money to transfer " << money_to_transfer
                    << " USD to Bob\n";
          return {
            bank_error::insufficient_funds,
            "not enough funds on Alice's account",
          };
        }
        alice_content.balance -= money_to_transfer;
        bob_content.balance += money_to_transfer;

        {
          auto [e5, a] = ctx->replace(alice, alice_content);
          if (e5.ec()) {
            std::cout << "Unable to update account for Alice: " << e5.ec().message() << "\n";
          }
        }
        {
          auto [e6, b] = ctx->replace(bob, bob_content);
          if (e6.ec()) {
            std::cout << "Unable to update account for Bob: " << e6.ec().message() << "\n";
          }
        }
        return {};
      });

    if (err.ec()) {
      std::cout << "Transaction has failed: " << err.ec().message() << "\n";
      if (auto cause = err.cause(); cause.has_value()) {
        std::cout << "Cause: " << cause->ec().message() << "\n";
      }
      return EXIT_FAILURE;
    }
  }

  {
    auto [err, resp] = collection.get("alice", {}).get();
    if (err.ec()) {
      std::cout << "Unable to read account for Alice: " << err.message() << "\n";
      return EXIT_FAILURE;
    }
    std::cout << "Alice (CAS=" << resp.cas().value() << "): " << resp.content_as<bank_account>()
              << "\n";
  }
  {
    auto [err, resp] = collection.get("bob", {}).get();
    if (err.ec()) {
      std::cout << "Unable to read account for Bob: " << err.message() << "\n";
      return EXIT_FAILURE;
    }
    std::cout << "Bob (CAS=" << resp.cas().value() << "): " << resp.content_as<bank_account>()
              << "\n";
  }

#endif
  cluster.close().get();

  return 0;
}

auto
program_config::from_env() -> program_config
{
  program_config config{};

  if (const auto* val = getenv("CONNECTION_STRING"); val != nullptr) {
    config.connection_string = val;
  }
  if (const auto* val = getenv("USER_NAME"); val != nullptr) {
    config.user_name = val;
  }
  if (const auto* val = getenv("PASSWORD"); val != nullptr) {
    config.password = val;
  }
  if (const auto* val = getenv("PREFERRED_SERVER_GROUP"); val != nullptr) {
    config.preferred_server_group = val;
  }
  if (const auto* val = getenv("BUCKET_NAME"); val != nullptr) {
    config.bucket_name = val;
  }
  if (const auto* val = getenv("SCOPE_NAME"); val != nullptr) {
    config.scope_name = val;
  }
  if (const auto* val = getenv("COLLECTION_NAME"); val != nullptr) {
    config.collection_name = val;
  }
  if (const auto* val = getenv("PROFILE"); val != nullptr) {
    config.profile = val; // e.g. "wan_development"
  }
  if (const auto* val = getenv("VERBOSE"); val != nullptr) {
    const std::array<std::string, 5> truthy_values = {
      "yes", "y", "on", "true", "1",
    };
    for (const auto& truth : truthy_values) {
      if (val == truth) {
        config.verbose = true;
        break;
      }
    }
  }

  return config;
}

auto
program_config::quote(std::string val) -> std::string
{
  return "\"" + val + "\"";
}

void
program_config::dump()
{
  std::cout << "       CONNECTION_STRING: " << quote(connection_string) << "\n";
  std::cout << "               USER_NAME: " << quote(user_name) << "\n";
  std::cout << "                PASSWORD: [HIDDEN]\n";
  std::cout << "  PREFERRED_SERVER_GROUP: " << quote(preferred_server_group) << "\n";
  std::cout << "             BUCKET_NAME: " << quote(bucket_name) << "\n";
  std::cout << "              SCOPE_NAME: " << quote(scope_name) << "\n";
  std::cout << "         COLLECTION_NAME: " << quote(collection_name) << "\n";
  std::cout << "                 VERBOSE: " << std::boolalpha << verbose << "\n";
  std::cout << "                 PROFILE: " << (profile ? quote(*profile) : "[NONE]") << "\n\n";
}

namespace generator
{
namespace
{
static const uint32_t crc32tab[256] = {
  0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f, 0xe963a535, 0x9e6495a3,
  0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988, 0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91,
  0x1db71064, 0x6ab020f2, 0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
  0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9, 0xfa0f3d63, 0x8d080df5,
  0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172, 0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b,
  0x35b5a8fa, 0x42b2986c, 0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
  0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423, 0xcfba9599, 0xb8bda50f,
  0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924, 0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d,
  0x76dc4190, 0x01db7106, 0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
  0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d, 0x91646c97, 0xe6635c01,
  0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e, 0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457,
  0x65b0d9c6, 0x12b7e950, 0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
  0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7, 0xa4d1c46d, 0xd3d6f4fb,
  0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0, 0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9,
  0x5005713c, 0x270241aa, 0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
  0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81, 0xb7bd5c3b, 0xc0ba6cad,
  0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a, 0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683,
  0xe3630b12, 0x94643b84, 0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
  0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb, 0x196c3671, 0x6e6b06e7,
  0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc, 0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5,
  0xd6d6a3e8, 0xa1d1937e, 0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
  0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55, 0x316e8eef, 0x4669be79,
  0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236, 0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f,
  0xc5ba3bbe, 0xb2bd0b28, 0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
  0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f, 0x72076785, 0x05005713,
  0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38, 0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21,
  0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
  0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69, 0x616bffd3, 0x166ccf45,
  0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2, 0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db,
  0xaed16a4a, 0xd9d65adc, 0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
  0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693, 0x54de5729, 0x23d967bf,
  0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94, 0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d,
};

auto
hash_crc32(const std::string& data) -> std::uint32_t
{
  std::uint32_t crc = std::numeric_limits<std::uint32_t>::max();

  for (const auto x : data) {
    crc = (crc >> 8) ^ crc32tab[(crc ^ static_cast<std::uint32_t>(x)) & 0xff];
  }
  return ((~crc) >> 16) & 0x7fff;
}

using vbucket_id = std::uint16_t;
constexpr std::size_t default_number_of_vbuckets{ 1024 };

auto
map_key_to_vbucket_id(const std::string& key, std::size_t number_of_vbuckets) -> vbucket_id
{
  auto digest = hash_crc32(key);
  return static_cast<vbucket_id>(digest % number_of_vbuckets);
}

static const std::array alphabet = {
  '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
  'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
  'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
  'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
};

auto
random_text(std::size_t length) -> std::string
{
  static thread_local std::minstd_rand gen{ std::random_device()() };
  std::string text(length, '-');
  for (std::size_t i = 0; i < length; ++i) {
    text[i] = alphabet[gen() % alphabet.size()];
  }
  return text;
}

class short_key_generator
{
public:
  short_key_generator(const std::string& prefix)
    : prefix_{ prefix }
  {
  }

  auto next_key() -> std::string
  {
    update_state();
    return fmt::format("{}{}", prefix_, state_);
  }

private:
  void update_state()
  {
    for (std::size_t i = 0; i < state_.size(); ++i) {
      auto it = std::find(alphabet.begin(), alphabet.end(), state_[i]);
      if (it == alphabet.end() - 1) {
        state_[i] = alphabet.front();
      } else {
        state_[i] = *(it + 1);
        return;
      }
    }

    state_.push_back(alphabet.front());
  }

  std::string prefix_;
  std::string state_{};
};
} // namespace

auto
keys_for_vbuckets(const std::string& prefix,
                  std::size_t number_of_vbuckets,
                  std::size_t number_of_keys_per_vbucket) -> std::vector<std::string>
{
  const std::size_t number_of_keys_required{ number_of_vbuckets * number_of_keys_per_vbucket };

  short_key_generator generator{ prefix };
  std::map<vbucket_id, std::vector<std::string>> key_groups{};
  std::size_t number_of_keys_generated{ 0 };
  while (number_of_keys_generated < number_of_keys_required) {
    auto key = generator.next_key();
    auto vbucket = map_key_to_vbucket_id(key, number_of_vbuckets);
    if (key_groups[vbucket].size() >= number_of_keys_per_vbucket) {
      continue;
    }
    key_groups[vbucket].push_back(key);
    ++number_of_keys_generated;
  }

  std::vector<std::string> result;
  result.reserve(number_of_keys_generated);
  for (const auto& [_, group] : key_groups) {
    std::copy(group.begin(), group.end(), std::back_inserter(result));
  }

  return result;
}

auto
keys(const std::string& prefix, std::size_t number_of_keys) -> std::vector<std::string>
{
  static thread_local std::minstd_rand engine{ std::random_device()() };

  auto result = keys_for_vbuckets(
    prefix, default_number_of_vbuckets, number_of_keys / default_number_of_vbuckets + 1);
  std::shuffle(result.begin(), result.end(), engine);
  if (result.size() > number_of_keys) {
    result.resize(number_of_keys);
  }
  return result;
}

auto
json_objects(std::size_t number_of_objects, std::size_t minimum_size_of_object)
  -> std::vector<tao::json::value>
{
  std::vector<tao::json::value> result;
  result.reserve(number_of_objects);
  for (std::size_t i = 0; i < number_of_objects; ++i) {
    result.emplace_back(tao::json::value{
      { "index", i },
      { "size", minimum_size_of_object },
      { "text", random_text(minimum_size_of_object) },
    });
  }
  return result;
}

auto
encoded_json_objects(std::size_t number_of_objects, std::size_t minimum_size_of_object)
  -> std::vector<std::string>
{
  std::vector<std::string> result;
  result.reserve(number_of_objects);
  for (const auto& object : json_objects(number_of_objects, minimum_size_of_object)) {
    result.emplace_back(tao::json::to_string(object));
  }
  return result;
}

auto
encoded_csv_tables(std::size_t number_of_objects, std::size_t minimum_size_of_table)
  -> std::vector<std::string>
{
  std::vector<std::string> result;
  result.reserve(number_of_objects);
  for (std::size_t i = 0; i < number_of_objects; ++i) {
    std::string table;
    table.reserve(minimum_size_of_table);
    constexpr std::size_t number_of_rows{ 10 };
    for (std::size_t r = 0; r < number_of_rows; ++r) {
      table += std::to_string(r) + "," + random_text(minimum_size_of_table / number_of_rows) + "\n";
    }
    result.emplace_back(table);
  }
  return result;
}

auto
json_test_space(std::size_t number_of_objects, std::size_t minimum_size_of_object)
  -> test_space<tao::json::value>
{
  const auto prefix = fmt::format("json_{}_{}_", number_of_objects, minimum_size_of_object);

  auto begin = std::chrono::high_resolution_clock::now();

  test_space<tao::json::value> result = {
    fmt::format("JSON, {} objects, {} bytes min size", number_of_objects, minimum_size_of_object),
    keys(prefix, number_of_objects),
    json_objects(number_of_objects, minimum_size_of_object),
  };

  auto end = std::chrono::high_resolution_clock::now();

  fmt::println("{}: generated in {} ({}/doc)",
               result.description,
               std::chrono::duration_cast<std::chrono::milliseconds>(end - begin),
               std::chrono::duration_cast<std::chrono::milliseconds>(end - begin) / number_of_objects);
  return result;
}

auto
encoded_json_test_space(std::size_t number_of_objects, std::size_t minimum_size_of_object)
  -> test_space<std::string>
{
  const auto prefix = fmt::format("encoded_json_{}_{}_", number_of_objects, minimum_size_of_object);

  return {
    fmt::format(
      "encoded JSON, {} objects, {} bytes min size", number_of_objects, minimum_size_of_object),
    keys(prefix, number_of_objects),
    encoded_json_objects(number_of_objects, minimum_size_of_object),
  };
}

auto
encoded_csv_test_space(std::size_t number_of_objects, std::size_t minimum_size_of_table)
  -> test_space<std::string>
{
  const auto prefix = fmt::format("encoded_csv_{}_{}_", number_of_objects, minimum_size_of_table);

  return {
    fmt::format(
      "encoded CSV, {} objects, {} bytes min size", number_of_objects, minimum_size_of_table),
    keys(prefix, number_of_objects),
    encoded_csv_tables(number_of_objects, minimum_size_of_table),
  };
}
} // namespace generator

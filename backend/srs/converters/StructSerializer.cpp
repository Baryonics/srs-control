#include "StructSerializer.hpp"
#include "srs/converters/DataConverterBase.hpp"
#include "srs/data/SRSDataCompact.hpp"
#include "srs/data/SRSDataStructs.hpp"
#include "srs/utils/CommonAlias.hpp"
#include "srs/utils/CommonDefinitions.hpp"
#include "srs/utils/CommonFunctions.hpp"
#include <algorithm>
#include <array>
#include <bit>
#include <bitset>
#include <boost/asio/any_io_executor.hpp>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <vector>
#include <zpp_bits.h>

namespace srs::process
{
    namespace
    {
        auto marker_to_compact(const MarkerData& marker_data, internal::MarkerDataCompact& marker_data_compact)
            -> std::expected<void, std::string_view>
        {
            if (marker_data.vmm_id >= (1U << internal::VMM_ID_BIT_LENGTH))
            {
                return std::unexpected("Serialization: vmm_id exceeding size limit!");
            }
            if (marker_data.srs_timestamp >
                1ULL << (common::SRS_TIMESTAMP_LOW_BIT_LENGTH + common::SRS_TIMESTAMP_HIGH_BIT_LENGTH))
            {
                return std::unexpected("Serialization: timestamp exceeding size limit!");
            }
            auto timestamp_bits =
                std::bitset<common::SRS_TIMESTAMP_HIGH_BIT_LENGTH + common::SRS_TIMESTAMP_LOW_BIT_LENGTH>(
                    marker_data.srs_timestamp);
            auto [timestamp_low_bits, timestamp_high_bits] =
                common::split_bits<common::SRS_TIMESTAMP_LOW_BIT_LENGTH>(timestamp_bits);
            marker_data_compact.timestamp_low_bits =
                static_cast<decltype(marker_data_compact.timestamp_low_bits)>(timestamp_low_bits.to_ulong());
            marker_data_compact.timestamp_high_bits =
                static_cast<decltype(marker_data_compact.timestamp_high_bits)>(timestamp_high_bits.to_ulong());
            marker_data_compact.flag = static_cast<decltype(marker_data_compact.flag)>(0);
            marker_data_compact.vmm_id = static_cast<decltype(marker_data_compact.vmm_id)>(marker_data.vmm_id);
            return {};
        }

        auto hit_to_compact(const HitData& hit_data, internal::HitDataCompact& hit_data_compact)
            -> std::expected<void, std::string_view>
        {
            if (hit_data.channel_num >= 1U << internal::CHANNEL_NUM_BIT_LENGTH)
            {
                return std::unexpected("Serialization: channel_num exceeding size limit!");
            }
            if (hit_data.bc_id >= 1U << internal::BC_ID_BIT_LENGTH)
            {
                return std::unexpected("Serialization: bc_id exceeding size limit!");
            }
            if (hit_data.adc >= 1U << internal::ADC_BIT_LENGTH)
            {
                return std::unexpected("Serialization: adc exceeding size limit!");
            }
            if (hit_data.vmm_id >= 1U << internal::VMM_ID_BIT_LENGTH)
            {
                return std::unexpected("Serialization: vmm_id exceeding size limit!");
            }
            if (hit_data.offset >= 1U << internal::OFFSET_BIT_LENGTH)
            {
                return std::unexpected("Serialization: offset exceeding size limit!");
            }
            hit_data_compact.vmm_id = static_cast<decltype(hit_data_compact.vmm_id)>(hit_data.vmm_id);
            hit_data_compact.flag = static_cast<decltype(hit_data_compact.flag)>(1);
            hit_data_compact.adc = static_cast<decltype(hit_data_compact.adc)>(hit_data.adc);
            hit_data_compact.bc_id = static_cast<decltype(hit_data_compact.bc_id)>(hit_data.bc_id);
            hit_data_compact.bc_id = common::binary_to_gray(hit_data_compact.bc_id);
            hit_data_compact.channel_num = static_cast<decltype(hit_data_compact.channel_num)>(hit_data.channel_num);
            hit_data_compact.is_over_threshold =
                static_cast<decltype(hit_data_compact.is_over_threshold)>(hit_data.is_over_threshold);
            hit_data_compact.offset = static_cast<decltype(hit_data_compact.offset)>(hit_data.offset);
            return {};
        }

        template <class T>
            requires(std::same_as<std::remove_cvref_t<T>, internal::HitDataCompact> ||
                     std::same_as<std::remove_cvref_t<T>, internal::MarkerDataCompact>)
        auto compact_to_vector(const T& compact_data) -> std::vector<char>
        {
            auto compact_bitset = std::bitset<sizeof(std::uint64_t)>{ std::bit_cast<uint64_t>(compact_data) };
            auto output = std::vector<char>{};
            output.resize(sizeof(std::uint64_t) / common::BYTE_BIT_LENGTH);
            auto write_to_output = zpp::bits::out{ output, zpp::bits::endian::network{}, zpp::bits::no_size{} };
            write_to_output(compact_bitset).or_throw();
            output.resize(common::HIT_DATA_BIT_LENGTH / common::BYTE_BIT_LENGTH);
            return output;
        }
    }; // namespace

    StructSerializer::StructSerializer(size_t n_lines)
        : ConverterTask{ "Struct deserializer", none, n_lines }
    {
        output_data_.resize(n_lines);
    }

    // NOLINTBEGIN
    auto StructSerializer::convert([[maybe_unused]] const StructData* input, [[maybe_unused]] std::vector<char>& output)
        -> std::expected<std::size_t, std::string_view>
    {
        auto serialize_to = zpp::bits::out{ output, zpp::bits::endian::network{}, zpp::bits::no_size{} };
        for (auto hit : input->hit_data)
        {
            auto hit_compact = internal::HitDataCompact{};
            if (auto r = hit_to_compact(hit, hit_compact); !r)
            {
                return std::unexpected(r.error());
            }
            serialize_to(compact_to_vector(hit_compact)).or_throw();
        }
        for (auto marker : input->marker_data)
        {
            auto marker_compact = internal::MarkerDataCompact{};
            if (auto r = marker_to_compact(marker, marker_compact); !r)
            {
                return std::unexpected(r.error());
            }
            serialize_to(compact_to_vector(marker_compact)).or_throw();
        }
        return input->marker_data.size() + input->hit_data.size();
    }
    // NOLINTEND
} // namespace srs::process

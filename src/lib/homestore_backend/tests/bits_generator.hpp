/*********************************************************************************
 * Modifications Copyright 2017-2019 eBay Inc.
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/

// Copied from homestore.

#pragma once
#include <type_traits>
#include <cstdint>
#include <random>
#include <iostream>

namespace homeobject {

class BitsGenerator {
public:
    static void gen_random_bits(size_t size, uint8_t* buf) {
        std::random_device rd;
        std::default_random_engine g(rd());
        std::uniform_int_distribution< unsigned long long > dis(std::numeric_limits< std::uint8_t >::min(),
                                                                std::numeric_limits< std::uint8_t >::max());
        for (size_t i = 0; i < size; ++i) {
            buf[i] = dis(g);
        }
    }

    static void gen_random_bits(sisl::blob& b) { gen_random_bits(b.size, b.bytes); }
};

}; // namespace homeobject

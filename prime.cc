/* -*-Mode: c++;-*-
  Copyright (c) 2006-2008 John Plevyak, All Rights Reserved
*/
#include <cstdint>
#include <cstdlib>

#define PASSES 30

static uint32_t modular_exponent(uint32_t base, uint32_t power, uint32_t modulus) {
  uint64_t result = 1;
  for (int i = 31; i >= 0; i--) {
    result = (result * result) % modulus;
    if (power & (1 << i)) result = (result * base) % modulus;
  }
  return (uint32_t)result;
}

// true: possibly prime
static bool miller_rabin_pass(uint32_t a, uint32_t n) {
  uint32_t s = 0;
  uint32_t d = n - 1;
  while ((d % 2) == 0) {
    d /= 2;
    s++;
  }
  uint32_t a_to_power = modular_exponent(a, d, n);
  if (a_to_power == 1) return true;
  for (uint32_t i = 0; i < s - 1; i++) {
    if (a_to_power == n - 1) return true;
    a_to_power = modular_exponent(a_to_power, 2, n);
  }
  if (a_to_power == n - 1) return true;
  return false;
}

bool miller_rabin(uint32_t n) {
  uint32_t a = 0;
  for (int t = 0; t < PASSES; t++) {
    do {
      a = rand() % n;
    } while (!a);
    if (!miller_rabin_pass(a, n)) return false;
  }
  return true;
}

uint32_t next_higher_prime(uint32_t n) {
  if (!(n & 1)) n++;
  while (1) {
    if (miller_rabin(n)) return n;
    n += 2;
  }
}

uint32_t next_lower_prime(uint32_t n) {
  if (!(n & 1)) n--;
  while (1) {
    if (miller_rabin(n)) return n;
    n -= 2;
  }
}

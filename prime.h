/* -*-Mode: c++;-*-
  Based on http://primes.utm.edu
  Derivation Copyright (c) 2006-2008 John Plevyak, All Rights Reserved
*/
#ifndef prime_H
#define prime_H

#include <cstdint>

uint32_t next_higher_prime(uint32_t i);
uint32_t next_lower_prime(uint32_t i);
bool miller_rabin(uint32_t n);

#endif

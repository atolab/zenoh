#include "zenoh-ffi.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

int main(int argc, char** argv) {
  ZNSession *s = zn_open("", 0);
  if (s == 0) {
    printf("Error creating session!\n");
    exit(-1);
  } 
  sleep(1);
  const char *data = "01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567";
  const char *key = "/test/thr";
  printf("addr(data) = 0x%zx\n", (size_t)data);
  printf("addr(key) = 0x%zx\n", (size_t)key);
  while (1) {
    zn_write(s, key, data, strlen(data));
  }
}
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
  const char *data = "Hello from C";
  const char *key = "/zenoh/demo/quote";
  zn_write(s, key, data, strlen(data));
  zn_close(s);
}
#include "zenoh-ffi.h"
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

int main(int argc, char** argv) {
  size_t len;
  if (argc < 2) {
    printf("USAGE:\n\t thrput_test <size>\n");
    return -1;    
  }
  len = atoi(argv[1]);
  

  ZNSession *s = zn_open("", 0);
  if (s == 0) {
    printf("Error creating session!\n");
    exit(-1);
  } 
  sleep(1);
  char *data = (char*) malloc(len);
  memset(data, 1, len);
  printf("Running throughput test for %zu bytes payload.\n", len);
  const char *key = "/test/thr";
  size_t id = zn_declare_resource(s, key);  
  while (1) {
    zn_write_wrid(s, id, data, len);
  }
}
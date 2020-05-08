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
  char *data = "Hello World\0";
  zn_write(s, "/zenoh/demo/quote", data, strlen(data));
  sleep(3);
  zn_close(s);  
}
#include "strgen.h"

static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.-#'?!";        

int strgen(char* buffer, size_t buflen) {

    int charsetsize = (int)(sizeof(charset) -1);
    if (buffer) {            
        for (int n = 0;n < buflen;n++) {            
             int key = rand() % charsetsize;
             buffer[n] = charset[key];
            }

    }

    return true;
}

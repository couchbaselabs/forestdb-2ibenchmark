#include "strgen.h"

static char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,.-#'?!";        

int strgen(char* buffer, size_t buflen) {


    if (buffer) {            
        for (int n = 0;n < buflen;n++) {            
             int key = rand() % (int)(sizeof(charset) -1);
             buffer[n] = charset[key];
            }

    }

    return true;
}

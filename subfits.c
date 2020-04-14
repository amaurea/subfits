#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <error.h>
#include "slice_fits.h"

int main(int argc, char ** argv) {
	if(argc != 4) {
		fprintf(stderr, "Usage: subfits ifile pbox=y1:y2,x1:x2 ofile, or\n       subfits ifile box=dec1:dec2,ra1:ra2 ofile\n");
		exit(1);
	}
	char * ifile = argv[1], * sel = argv[2], * ofile = argv[3];
	int code = FSLICE_OK;
	int ifd = open(ifile, O_RDONLY);
	if(ifd < 0) { perror("ifile"); code = FSLICE_EIO; goto cleanup; }
	int ofd = open(ofile, O_WRONLY|O_CREAT|O_TRUNC, 0666);
	if(ofd < 0) { perror("ofile"); code = FSLICE_EIO; goto cleanup; }
	code = slice_fits(ifd, ofd, sel, NULL);
cleanup:
	if(ifd >= 0) close(ifd);
	if(ofd >= 0) close(ofd);
	if(code) fprintf(stderr, "Error code %d\n", code);
	return code;
}

#include <stddef.h>
enum { FSLICE_OK, FSLICE_EIO, FSLICE_EMAP, FSLICE_EPARSE, FSLICE_EALLOC, FSLICE_EVALS, FSLICE_OFD, FSLICE_UNKNOWN };
int slice_fits(int ifd, int ofd, char * sel, size_t * osize);

#include <sys/uio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <errno.h>
#include <limits.h>
#include <wcshdr.h>
#include "slice_fits.h"

#define true 1
#define false 0
#define HEADER_NROW 36
#define HEADER_NCOL 80
#define NAXIS_MAX 10
#define MAX_IOVEC 1024


typedef struct HeaderInfo {
	ssize_t bitpix_pos; ssize_t bitpix;
	ssize_t naxes_pos;            ssize_t naxes;
	ssize_t wcsaxes_pos;          ssize_t wcsaxes;
	ssize_t naxis_pos[NAXIS_MAX]; ssize_t naxis[NAXIS_MAX];
	ssize_t crpix_pos[NAXIS_MAX]; double  crpix[NAXIS_MAX];
	ssize_t cdelt_pos[NAXIS_MAX]; double  cdelt[NAXIS_MAX];
} HeaderInfo;

enum { SLICE_RANGE, SLICE_SINGLE };
typedef struct Slice {
	union { struct { ssize_t x1, y1; }; ssize_t i1[NAXIS_MAX]; };
	union { struct { ssize_t x2, y2; }; ssize_t i2[NAXIS_MAX]; };
	ssize_t mode[NAXIS_MAX];
	ssize_t naxes;
} Slice;

ssize_t idiv(ssize_t a, ssize_t b) { return a < 0 ? -((-a)/b) : a/b; }
ssize_t imod(ssize_t a, ssize_t b) { ssize_t c = a%b; if(c < 0) c += b; return c; }
ssize_t imax(ssize_t a, ssize_t b) { return a > b ? a : b; }
ssize_t imin(ssize_t a, ssize_t b) { return a < b ? a : b; }

int parse_header(char * header, HeaderInfo * info) {
	// Initialize to -1, so we can see if we have read them later
	info->naxes_pos = info->wcsaxes_pos = info->bitpix_pos = -1;
	for(int i = 0; i < NAXIS_MAX; i++)
		info->naxis_pos[i] = info->crpix_pos[i] = info->cdelt_pos[i] = -1;
	char nbuf[8+1];
	for(int ri = 0; ri < HEADER_NROW; ri++) {
		char * name = header + ri*HEADER_NCOL;
		char * data = name   + 10;
		ssize_t doff = data - header;
		if     (!strncmp(name, "BITPIX  ", 8)) { info->bitpix_pos = doff; info->bitpix = atoi(data); }
		else if(!strncmp(name, "NAXIS   ", 8)) { info->naxes_pos  = doff; info->naxes  = imin(atoi(data), NAXIS_MAX); }
		else if(!strncmp(name, "WCSAXES ", 8)) { info->wcsaxes_pos= doff; info->wcsaxes= imin(atoi(data), NAXIS_MAX); }
		else if(!strncmp(name, "NAXIS", 5)) {
			int ax = atoi(name+5)-1;
			if(ax < 0 || ax >= NAXIS_MAX) return false;
			info->naxis_pos[ax] = doff;
			info->naxis[ax]     = atoi(data);
		}
		else if(!strncmp(name, "CRPIX", 5)) {
			int ax = atoi(name+5)-1;
			if(ax < 0 || ax >= NAXIS_MAX) return false;
			info->crpix_pos[ax] = doff;
			info->crpix[ax]     = atof(data);
		}
		else if(!strncmp(name, "CDELT", 5)) {
			int ax = atoi(name+5)-1;
			if(ax < 0 || ax >= NAXIS_MAX) return false;
			info->cdelt_pos[ax] = doff;
			info->cdelt[ax]     = atof(data);
		}
	}
	int ok = info->bitpix_pos != -1 && info->naxes_pos != -1;
	if(ok) for(int i = 0; i < info->naxes; i++)
		ok &= info->naxis_pos[i] != -1;
	if(ok) for(int i = 0; i < info->wcsaxes; i++)
		ok &= info->crpix_pos[i] != -1 && info->cdelt_pos[i] != -1;
	return ok;
}

void update_header(char * header, HeaderInfo * info) {
	char buf[21];
	snprintf(buf, 21, "%20d", info->bitpix);  memcpy(header+info->bitpix_pos, buf, 20);
	snprintf(buf, 21, "%20d", info->naxes);   memcpy(header+info->naxes_pos,  buf, 20);
	snprintf(buf, 21, "%20d", info->wcsaxes); memcpy(header+info->wcsaxes_pos,buf, 20);
	for(int i = 0; i < info->naxes; i++) {
		snprintf(buf, 21, "%20d",    info->naxis[i]); memcpy(header+info->naxis_pos[i], buf, 20);
	}
	for(int i = 0; i < info->wcsaxes; i++) {
		snprintf(buf, 21, "%20.8f",  info->crpix[i]); memcpy(header+info->crpix_pos[i], buf, 20);
		snprintf(buf, 21, "%20.15f", info->cdelt[i]); memcpy(header+info->cdelt_pos[i], buf, 20);
	}
}

void prune_header(char * iheader, char * oheader, int naxes) {
	// Copy iheader to oheader, but remove NAXISX entries that
	// are higher than naxes.
	int naxisX;
	memset(oheader, ' ', HEADER_NROW*HEADER_NCOL);
	for(int i = 0, j = 0; i < HEADER_NROW; i++, j++) {
		char * irow = iheader + i*HEADER_NCOL;
		char * orow = oheader + j*HEADER_NCOL;
		if(sscanf(irow, "NAXIS%d ", &naxisX) == 1 && naxisX > naxes) j--;
		else memcpy(orow, irow, HEADER_NCOL);
	}
}

int parse_sel(char * sel, HeaderInfo * info, char * header, Slice * slice) {
	// Turn a selector of the form pbox=...,y1:y2,x1:x2 or box=...,dec1:dec2,ra1:ra2 into
	// a Slice. Header is only needed for the box case, in which case wcslib will
	// be used to convert the coordinates to pixel indices. The ... part is a simple slice
	// of any earlier dimensions
	
	// Initialize all axes to full slices
	slice->naxes = info->naxes;
	for(ssize_t i = 0; i < info->naxes; i++) {
		slice->mode[i] = SLICE_RANGE;
		slice->i1[i]   = 0;
		slice->i2[i]   = info->naxis[i];
	}
	// If sel is NULL or an empty string, don't do any slicing, just use the full array
	if(!sel || !*sel) return true;

	// Then parse. The slice is in the opposite order of our final slice object, so
	// we make a temporary array to read it into. We make it double so that we can
	// handle both box and pbox with the same parser
	int fix_order = false;
	double tmp_i1[NAXIS_MAX], tmp_i2[NAXIS_MAX];
	ssize_t tmp_mode[NAXIS_MAX], tmp_naxes = 0;
	char * c1 = sel, * c2;
	// First find the foo= part
	for(c2 = c1; *c2 && *c2 != '='; c2++);
	if(*c2 != '=') return false;
	char * name1 = c1, *name2 = c2;
	// Then loop over dimensions
	for(tmp_naxes = 0; tmp_naxes < NAXIS_MAX && *c2; tmp_naxes++) {
		// Advance to next comma or end of string
		c1 = c2+1;
		for(c2 = c1; *c2 && *c2 != ','; c2++);
		// We can have either a single number of a range
		int nread = sscanf(c1, "%lf:%lf", &tmp_i1[tmp_naxes], &tmp_i2[tmp_naxes]);
		// Don't allow single mode for the last axes
		if(nread == 0 || tmp_naxes >= info->naxes-2 && nread == 1) return false;
		else tmp_mode[tmp_naxes] = nread == 1 ? SLICE_SINGLE : SLICE_RANGE;
	}
	if(!strncmp(name1, "pbox", name2-name1)) {
		// No conversion necessary
	} else if(!strncmp(name1, "box", name2-name1)) {
		// Convert the degrees do pixels for the pixel axes. First build our wcs object from
		// the header.
		struct wcsprm * wcs;
		int nreject, nwcs, status;
		status = wcspih(header, HEADER_NROW, 0, 0, &nreject, &nwcs, &wcs);
		if(status) return false;
		// Am I supposed to have to set these manually? I don't remember wcslib being this
		// painful to use. All the 9s below (rather than 2) are there to avoid its naxis
		// convusion getting in the way.
		wcs->lng = 0; wcs->lat = 1;
		// Then convert the coordinates
		double world[2][9], phi[2], theta[2], imgcoord[2][9], pixcoord[2][9];
		world[0][0] = tmp_i1[tmp_naxes-1]; world[0][1] = tmp_i1[tmp_naxes-2];
		world[1][0] = tmp_i2[tmp_naxes-1]; world[1][1] = tmp_i2[tmp_naxes-2];
		int stat[2];
		status = wcss2p(wcs, 2, 9, world[0], phi, theta, imgcoord[0], pixcoord[0], stat);
		if(status) { wcsvfree(&nwcs, &wcs); return false; }
		tmp_i1[tmp_naxes-1] = floor(pixcoord[0][0]+0.5); tmp_i1[tmp_naxes-2] = floor(pixcoord[0][1]+0.5);
		tmp_i2[tmp_naxes-1] = floor(pixcoord[1][0]+0.5); tmp_i2[tmp_naxes-2] = floor(pixcoord[1][1]+0.5);
		wcsvfree(&nwcs, &wcs);
		// fits coordinate ambiguity might have led to wrapping, which we must undo
		fix_order = true;
	}
	// Finally copy into the output slice
	for(ssize_t i = 0; i < tmp_naxes; i++) {
		slice->i1[tmp_naxes-1-i]   = (int)tmp_i1[i];
		slice->i2[tmp_naxes-1-i]   = (int)tmp_i2[i];
		slice->mode[tmp_naxes-1-i] = (int)tmp_mode[i];
	}
	for(ssize_t i = 0; i < slice->naxes; i++)
		if(slice->mode[i] == SLICE_SINGLE)
			slice->i2[i] = slice->i1[i]+1;
	if(fix_order) {
		int wrapx = (int)(fabs(360/info->cdelt[0])+0.5);
		if(slice->i2[0] < slice->i1[0]) slice->i2[0] += wrapx;
	}
	return true;
}

typedef struct WriteQueue {
	int fd;
	ssize_t n;
	struct iovec ios[MAX_IOVEC];
} WriteQueue;

int push_write(WriteQueue * queue, void * buf, ssize_t len) {
	if(queue->n >= MAX_IOVEC || buf == NULL) {
		// Perform the write. This is almost always just a single writev operation, but can be
		// less if a signal interrupted a blocked write, or if the disk was full. All this extra
		// code is there to support that situation
		ssize_t ntot = 0;
		for(ssize_t i = 0; i < queue->n; i++) ntot += queue->ios[i].iov_len;
		// Then loop over writev until we're done
		ssize_t nwrite_tot = 0;
		ssize_t bufi = 0;
		do {
			ssize_t nwrite = writev(queue->fd, queue->ios+bufi, queue->n-bufi);
			if(nwrite < 0) { perror("writev"); return false; }
			nwrite_tot += nwrite;
			while(queue->ios[bufi].iov_len < nwrite) {
				nwrite -= queue->ios[bufi].iov_len;
				bufi++;
			}
			queue->ios[bufi].iov_base += nwrite;
			queue->ios[bufi].iov_len  -= nwrite;
		} while(nwrite_tot < ntot);
		queue->n = 0;
	}
	if(buf) {
		queue->ios[queue->n].iov_base = buf;
		queue->ios[queue->n].iov_len  = len;
		queue->n++;
	}
	return true;
}

int slice_fits(int ifd, int ofd, char * sel, size_t * osize) {
	// Set up a memory map of the whole input file. We do this because we will use
	// the mmap with writev do do the whole read/write operation in a single
	// call.
	void * zeros = 0, * data = 0;
	int code = FSLICE_UNKNOWN;
	size_t flen = lseek(ifd, 0, SEEK_END); lseek(ifd, 0, SEEK_SET);
	data = mmap(NULL, flen, PROT_READ, MAP_PRIVATE, ifd, 0);
	if(!data) { code = FSLICE_EMAP; goto cleanup; }

	// Extract the fits header, and the part of the data we need for our index calculations
	char header[HEADER_NROW*HEADER_NCOL];
	memcpy(header, data, sizeof header);
	HeaderInfo info;
	if(!parse_header(header, &info)) { code = FSLICE_EPARSE; goto cleanup; }
	ssize_t nbyte = abs(info.bitpix)/8;
	Slice slice;
	if(!parse_sel(sel, &info, header, &slice)) { code = FSLICE_EPARSE; goto cleanup; }

	// Get our sky wrap info. This assumes a cylindrical projection
	ssize_t wrapy = 0, wrapx = (ssize_t)(fabs(360/info.cdelt[0])+0.5);
	// We don't allow selections that are bigger than the whole sky. We could,
	// but it's tedious to implement and confuses other fits code
	if(wrapy && slice.y2-slice.y1 > wrapy) { code = FSLICE_EVALS; goto cleanup; }
	if(wrapx && slice.x2-slice.x1 > wrapx) { code = FSLICE_EVALS; goto cleanup; }
	// Slices must be in the right order, and that none of the pre-dimensions are
	// out of bound
	for(ssize_t i = 0; i < slice.naxes; i++)
		if(slice.i2[i] < slice.i1[i] || (i >= 2 && (slice.i1[i] < 0 || slice.i2[i] > info.naxis[i])))
			{ code = FSLICE_EVALS; goto cleanup; }

	ssize_t ny = slice.y2 - slice.y1, nx = slice.x2 - slice.x1;

	// We know how big the response will be now
	if(osize) {
		*osize = 1;
		for(ssize_t i = 0; i < slice.naxes; i++)
			*osize *= slice.i2[i]-slice.i1[i];
		*osize = *osize*nbyte + HEADER_NROW*HEADER_NCOL;
	}
	// Abort on invalid ofd. It's useful to let this happen as late as this,
	// as it allows us to perform a trial run testing the validity of the slice
	// etc. Without performing any actual output. If everything else is OK, then
	// the return code will be FSLICE_OFD
	if(ofd < 0) { code = FSLICE_OFD; goto cleanup; }

	// Set up the output header. The main complication is the crpix shift.
	HeaderInfo oinfo = info;
	oinfo.naxis[0] = nx;
	oinfo.naxis[1] = ny;
	oinfo.crpix[0]-= slice.x1;
	oinfo.crpix[1]-= slice.y1;
	// Apply other slices
	for(ssize_t i = 2, j = 2; i < slice.naxes; i++, j++) {
		if(slice.mode[i] == SLICE_SINGLE) { oinfo.naxes--; j--; }
		else { oinfo.naxis[j] = slice.i2[i]-slice.i1[i]; }
	}
	update_header(header, &oinfo);
	char oheader[HEADER_NROW*HEADER_NCOL];
	prune_header(header, oheader, oinfo.naxes);

	WriteQueue queue = { ofd, 0 };
	push_write(&queue, oheader, HEADER_NROW*HEADER_NCOL);
	// Allocate a zero vector that we will use for missing data
	zeros = calloc(nx, nbyte);
	void * img_start = data + HEADER_NROW*HEADER_NCOL;
	if(!zeros) { code = FSLICE_EALLOC; goto cleanup; }
	// any-dimensional loop over pre-axes. We will loop over only the
	// valid values, so pre_inds is the offset from the slice starts slice.i1,
	// and pre_lens is the number of sliced values along each axis. At the bottom
	// of the do loop we count up and exit when ax overflow to slice.naxes-2.
	ssize_t pre_inds[NAXIS_MAX-2], pre_lens[NAXIS_MAX-2], ax;
	for(ssize_t ax = 0; ax < slice.naxes-2; ax++) {
		pre_inds[ax] = 0;
		pre_lens[ax] = slice.mode[ax+2] == SLICE_SINGLE ? 1 : slice.i2[ax+2]-slice.i1[ax+2];
	}
	do {
		// Get the full 1d index. This could be done more efficiently, but it will be
		// subdominant anyway
		ssize_t ipre = 0;
		for(ssize_t ax = slice.naxes-2-1; ax >=0 ; ax--)
			ipre = ipre * info.naxis[ax+2] + slice.i1[ax+2] + pre_inds[ax];

		for(ssize_t ly = slice.y1; ly < slice.y2; ly++) {
			ssize_t y = wrapy ? imod(ly, wrapy) : ly;
			if(y < 0 || y >= info.naxis[1]) { if(!push_write(&queue, zeros, nx*nbyte)) { code = FSLICE_EIO; goto cleanup; } }
			else {
				void * rdata = img_start + ((info.naxis[1]*ipre+y)*info.naxis[0])*nbyte;

				ssize_t nloop = wrapx ? idiv(slice.x2, wrapx) : 0;
				ssize_t x = slice.x1 - nloop*wrapx, x2 = slice.x2 - nloop*wrapx;
				// Handling sky wrapping is tedious!
				if(x < 0 && wrapx && x < info.naxis[0]-wrapx) {
					// We see the end of the patch wrapping around to the left
					ssize_t n = info.naxis[0]-wrapx-x;
					if(!push_write(&queue, rdata+(info.naxis[0]-n)*nbyte, n*nbyte)) {  code = FSLICE_EIO; goto cleanup; }
					x += n;
				}
				if(x < 0) {
					// Empty area to the left
					ssize_t n = -x;
					if(!push_write(&queue, zeros, n*nbyte)) {  code = FSLICE_EIO; goto cleanup; }
					x += n;
				}
				if(x < info.naxis[0]) {
					// We're inside the main part of the image
					ssize_t n = imin(x2,info.naxis[0])-x;
					if(!push_write(&queue, rdata+x*nbyte, n*nbyte)) {  code = FSLICE_EIO; goto cleanup; }
					x += n;
				}
				if(x < x2) {
					// Empty stuff to our right
					ssize_t n = x2-x;
					if(!push_write(&queue, zeros, n*nbyte)) {  code = FSLICE_EIO; goto cleanup; }
					x += n;
				}
			}
		}
		// Update any-dimensional counter
		for(ax = 0; ax < slice.naxes-2; ax++)
			if(++pre_inds[ax] < pre_lens[ax]) break;
			else pre_inds[ax] = 0;
	} while(ax < slice.naxes-2);
	// Write whatever's left in the queue
	if(!push_write(&queue, NULL, 0)) { code = FSLICE_EIO; goto cleanup; }

	code = FSLICE_OK;

cleanup:
	if(zeros)free(zeros);
	if(data) munmap(data, flen);
	return code;
}

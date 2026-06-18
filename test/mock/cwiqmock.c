// cwiqmock.c — LD_PRELOAD shim that mocks getxattr() for the cwiqduck test suite.
//
// The extension detects CWIQ-FS-backed files by reading the `system.cwiqfs.s3_url`
// xattr. The `system.*` namespace cannot be set by an unprivileged user on a normal
// filesystem, so tests cannot create a real fixture with that attribute. This shim
// interposes getxattr() and, for paths listed in a config file, returns a fake S3 URL
// (or injects a specific errno) — leaving every other getxattr() call untouched.
//
// Config: env var CWIQ_MOCK_CONFIG points to a file with TAB-separated lines:
//     /abs/path/to/mount/file<TAB>file:///abs/path/to/target/file
//     /abs/path/to/denied/file<TAB>ERRNO=EACCES
//     /abs/path/to/empty/file<TAB>EMPTY
// A "URL=" prefix on the value is accepted and stripped for readability but optional.
// Only the `system.cwiqfs.s3_url` attribute name is intercepted; all other names and
// all unlisted paths fall through to the real getxattr().

#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/xattr.h>

#define CWIQ_XATTR_NAME "system.cwiqfs.s3_url"
#define MAX_ENTRIES     64
#define MAX_LINE        4096

typedef struct {
	char path[2048];
	char value[2048]; // resolved URL, or sentinel like "ERRNO=EACCES" / "EMPTY"
} mock_entry;

static mock_entry g_entries[MAX_ENTRIES];
static int g_n_entries = 0;
static int g_loaded = 0;

typedef ssize_t (*getxattr_fn)(const char *, const char *, void *, size_t);

static ssize_t real_getxattr(const char *path, const char *name, void *value, size_t size) {
	static getxattr_fn real = NULL;
	if (!real) {
		real = (getxattr_fn)dlsym(RTLD_NEXT, "getxattr");
	}
	return real(path, name, value, size);
}

static void load_config_once(void) {
	if (g_loaded) {
		return;
	}
	g_loaded = 1;
	const char *cfg = getenv("CWIQ_MOCK_CONFIG");
	if (!cfg) {
		return;
	}
	FILE *f = fopen(cfg, "r");
	if (!f) {
		return;
	}
	char line[MAX_LINE];
	while (fgets(line, sizeof(line), f) && g_n_entries < MAX_ENTRIES) {
		// strip trailing newline
		size_t len = strlen(line);
		while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
			line[--len] = '\0';
		}
		if (len == 0 || line[0] == '#') {
			continue;
		}
		char *tab = strchr(line, '\t');
		if (!tab) {
			continue;
		}
		*tab = '\0';
		const char *path = line;
		const char *value = tab + 1;
		if (strncmp(value, "URL=", 4) == 0) {
			value += 4;
		}
		mock_entry *e = &g_entries[g_n_entries++];
		snprintf(e->path, sizeof(e->path), "%s", path);
		snprintf(e->value, sizeof(e->value), "%s", value);
	}
	fclose(f);
}

static const mock_entry *lookup(const char *path) {
	for (int i = 0; i < g_n_entries; i++) {
		if (strcmp(g_entries[i].path, path) == 0) {
			return &g_entries[i];
		}
	}
	return NULL;
}

static int errno_from_name(const char *name) {
	if (strcmp(name, "ENODATA") == 0)
		return ENODATA;
	if (strcmp(name, "ENOENT") == 0)
		return ENOENT;
	if (strcmp(name, "EACCES") == 0)
		return EACCES;
	if (strcmp(name, "ENOTSUP") == 0)
		return ENOTSUP;
	return EIO;
}

ssize_t getxattr(const char *path, const char *name, void *value, size_t size) {
	load_config_once();

	if (!name || strcmp(name, CWIQ_XATTR_NAME) != 0) {
		return real_getxattr(path, name, value, size);
	}

	const mock_entry *e = lookup(path);
	if (!e) {
		return real_getxattr(path, name, value, size);
	}

	// Error injection: value sentinel "ERRNO=<NAME>".
	if (strncmp(e->value, "ERRNO=", 6) == 0) {
		errno = errno_from_name(e->value + 6);
		return -1;
	}

	// Empty-value injection: attribute exists but is zero length.
	if (strcmp(e->value, "EMPTY") == 0) {
		return 0;
	}

	// Standard getxattr two-call protocol: size==0 means "tell me how big".
	size_t url_len = strlen(e->value);
	if (size == 0) {
		return (ssize_t)url_len;
	}
	if (size < url_len) {
		errno = ERANGE;
		return -1;
	}
	memcpy(value, e->value, url_len);
	return (ssize_t)url_len;
}

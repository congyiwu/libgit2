/*
 * Copyright (C) 2009-2012 the libgit2 contributors
 *
 * This file is part of libgit2, distributed under the GNU GPL v2 with
 * a Linking Exception. For full terms see the included COPYING file.
 */
#include "common.h"
#include "git2/types.h"
#include "git2/net.h"
#include "git2/object.h"
#include "git2/tag.h"
#include "git2/revwalk.h"
#include "git2/commit.h"
#include "git2/tree.h"
#include "refs.h"
#include "transport.h"
#include "posix.h"
#include "path.h"
#include "buffer.h"
#include "pkt.h"
#include "repository.h"

typedef struct {
	git_transport parent;
	git_repository *repo;
	/* oids in remote that need fetching */
	git_vector commit_oids_to_fetch;
} transport_local;

static int add_ref(transport_local *t, const char *name)
{
	const char peeled[] = "^{}";
	git_remote_head *head;
	git_object *obj = NULL, *target = NULL;
	git_transport *transport = (git_transport *) t;
	git_buf buf = GIT_BUF_INIT;
	git_pkt_ref *pkt;

	head = git__calloc(1, sizeof(git_remote_head));
	GITERR_CHECK_ALLOC(head);
	pkt = git__calloc(1, sizeof(git_pkt_ref));
	GITERR_CHECK_ALLOC(pkt);

	head->name = git__strdup(name);
	GITERR_CHECK_ALLOC(head->name);

	if (git_reference_name_to_oid(&head->oid, t->repo, name) < 0) {
		git__free(head->name);
		git__free(head);
		git__free(pkt);
		return -1;
	}

	pkt->type = GIT_PKT_REF;
	memcpy(&pkt->head, head, sizeof(git_remote_head));
	git__free(head);

	if (git_vector_insert(&transport->refs, pkt) < 0)
	{
		git__free(pkt->head.name);
		git__free(pkt);
		return -1;
	}

	/* If it's not a tag, we don't need to try to peel it */
	if (git__prefixcmp(name, GIT_REFS_TAGS_DIR))
		return 0;

	if (git_object_lookup(&obj, t->repo, &pkt->head.oid, GIT_OBJ_ANY) < 0)
		return -1;

	head = NULL;

	/* If it's not an annotated tag, just get out */
	if (git_object_type(obj) != GIT_OBJ_TAG) {
		git_object_free(obj);
		return 0;
	}

	/* And if it's a tag, peel it, and add it to the list */
	head = git__calloc(1, sizeof(git_remote_head));
	GITERR_CHECK_ALLOC(head);

	if (git_buf_join(&buf, 0, name, peeled) < 0)
		return -1;

	head->name = git_buf_detach(&buf);

	pkt = git__malloc(sizeof(git_pkt_ref));
	GITERR_CHECK_ALLOC(pkt);
	pkt->type = GIT_PKT_REF;

	if (git_tag_peel(&target, (git_tag *) obj) < 0)
		goto on_error;

	git_oid_cpy(&head->oid, git_object_id(target));
	git_object_free(obj);
	git_object_free(target);
	memcpy(&pkt->head, head, sizeof(git_remote_head));
	git__free(head);

	if (git_vector_insert(&transport->refs, pkt) < 0)
		return -1;

	return 0;

on_error:
	git_object_free(obj);
	git_object_free(target);
	return -1;
}

static int store_refs(transport_local *t)
{
	unsigned int i;
	git_strarray ref_names = {0};
	git_transport *transport = (git_transport *) t;

	assert(t);

	if (git_reference_list(&ref_names, t->repo, GIT_REF_LISTALL) < 0 ||
		git_vector_init(&transport->refs, (unsigned int)ref_names.count, NULL) < 0)
		goto on_error;

	/* Sort the references first */
	git__tsort((void **)ref_names.strings, ref_names.count, &git__strcmp_cb);

	/* Add HEAD */
	if (add_ref(t, GIT_HEAD_FILE) < 0)
		goto on_error;

	for (i = 0; i < ref_names.count; ++i) {
		if (add_ref(t, ref_names.strings[i]) < 0)
			goto on_error;
	}

	git_strarray_free(&ref_names);
	return 0;

on_error:
	git_vector_free(&transport->refs);
	git_strarray_free(&ref_names);
	return -1;
}

/*
 * Try to open the url as a git directory. The direction doesn't
 * matter in this case because we're calculating the heads ourselves.
 */
static int local_connect(git_transport *transport, int direction)
{
	git_repository *repo;
	int error;
	transport_local *t = (transport_local *) transport;
	const char *path;
	git_buf buf = GIT_BUF_INIT;

	GIT_UNUSED(direction);

	/* The repo layer doesn't want the prefix */
	if (!git__prefixcmp(transport->url, "file://")) {
		if (git_path_fromurl(&buf, transport->url) < 0) {
			git_buf_free(&buf);
			return -1;
		}
		path = git_buf_cstr(&buf);

	} else { /* We assume transport->url is already a path */
		path = transport->url;
	}

	error = git_repository_open(&repo, path);

	git_buf_free(&buf);

	if (error < 0)
		return -1;

	t->repo = repo;

	if (store_refs(t) < 0)
		return -1;

	t->parent.connected = 1;

	return 0;
}

/* This current implementation works by recursively reading objects from the
 * src repo and writing to the tgt repo.
 * TODO: When libgit2 supports creating packfiles, consider rewriting this to
 * share the smart protocol implementation used by the http and git transports.
 */
static int local_negotiate_fetch(git_transport *transport, git_repository *repo, const git_vector *wants)
{
	int ret;
	unsigned int i;
	git_oid oid;

	/* t->repo is the src */
	transport_local *t = (transport_local *) transport;
	git_revwalk *walk_src;

	/* repo parameter is the tgt */
	git_odb *odb_tgt;


	if ((ret = git_vector_init(&t->commit_oids_to_fetch, 16, NULL)) < 0)
		return ret;

	if ((ret = git_repository_odb__weakptr(&odb_tgt, repo)) < 0)
		return ret;

	if ((ret = git_revwalk_new(&walk_src, t->repo)) < 0)
		return ret;

	/* TODO: http.c uses GIT_SORT_TIME, who is right? */
	git_revwalk_sorting(walk_src, GIT_SORT_TOPOLOGICAL);

	/* get remote head oids missing in tgt */
	for (i = 0; i < wants->length; ++i) {
		git_remote_head *head = wants->contents[i];

		if (head->local) {
			if ((ret = git_revwalk_hide(walk_src, &head->oid)) < 0)
				goto cleanup;
		} else {
			if ((ret = git_revwalk_push(walk_src, &head->oid)) < 0)
				goto cleanup;
		}
	}

	/*
	 * Get all commit oids missing in tgt in topological order (newer ones before their parents).
	 * This assumes that if a commit exists in tgt, all of its parents exist as well.
	 */
	while ((ret = git_revwalk_next(&oid, walk_src)) == 0) {
		git_oid *new_oid;

		if (git_odb_exists(odb_tgt, &oid)) {
			if ((ret = git_revwalk_hide(walk_src, &oid)) < 0)
				goto cleanup;
		} else {
			if ((new_oid = git__malloc(sizeof(git_oid))) == NULL) {
				ret = -1;
				goto cleanup;
			}
			git_oid_cpy(new_oid, &oid);
			if (ret = git_vector_insert(&t->commit_oids_to_fetch, new_oid) < 0) {
				git__free(new_oid);
				goto cleanup;
			}
		}
	}

	if (ret == GIT_ITEROVER)
		ret = 0;

cleanup:
	git_revwalk_free(walk_src);
	return ret;
}

/* Copy an object from src repo to tgt repo if it does not exist in tgt yet. */
static int copy_object(git_odb *odb_src, git_odb *odb_tgt, const git_oid *oid, git_transfer_progress *stats)
{
	int ret;
	git_odb_stream *rstream = NULL;
	git_odb_stream *wstream = NULL;
	git_odb_object *obj = NULL;
	size_t size;
	git_otype type;
	char buffer[4096];
	size_t offset;
	size_t chunk_size;
	git_oid oid_recalc;

	if (git_odb_exists(odb_tgt, oid))
		return 0;

	/* prefer streaming reads */
	if (git_odb_open_rstream(&rstream, odb_src, oid) == 0) {
		if ((ret = git_odb_read_header(&size, &type, odb_src, oid)) < 0)
			goto cleanup;
	}
	/* 
	 * git_odb_open_rstream fails if the backend containing the oid does not support streaming reads.
	 * If this happens, fall back to non-streaming read.
	 */
	else {
		rstream = NULL;
		if ((ret = git_odb_read(&obj, odb_src, oid)) < 0)
			/* nothing was allocated yet in this loop, immediately return from function */
			return ret;

		size = git_odb_object_size(obj);
		type = git_odb_object_type(obj);
	}

	/* git_odb_open_wstream returns a wrapper stream if the primary backend does not support streaming writes */
	if ((ret = git_odb_open_wstream(&wstream, odb_tgt, size, type)) < 0) {
		wstream = NULL;
		goto cleanup;
	}

	for (offset = 0; offset < size; offset += chunk_size) {
		chunk_size = min(sizeof(buffer), size - offset);
		if (rstream) {
			if ((ret = rstream->read(rstream, buffer, chunk_size)) < 0)
				goto cleanup;

			if ((ret = wstream->write(wstream, buffer, chunk_size)) < 0)
				goto cleanup;
		}
		else {
			if ((ret = wstream->write(wstream, (char *)git_odb_object_data(obj) + offset, chunk_size)) < 0)
				goto cleanup;
		}
	}

	if ((ret = wstream->finalize_write(&oid_recalc, wstream)) < 0)
		goto cleanup;

	stats->received_bytes += size;

cleanup:
	if (wstream)
		wstream->free(wstream);
	if (obj)
		git_odb_object_free(obj);
	if (rstream)
		rstream->free(rstream);

	return ret;
}

/* Copy a tree and it's dependencies from src repo to tgt repo if they do not exist in tgt yet. */
static int copy_tree(git_repository *repo_src, git_odb *odb_src, git_odb *odb_tgt, const git_oid *oid, git_transfer_progress *stats)
{
	int ret;
	git_tree *tree;
	const git_tree_entry *entry;
	unsigned int i;

	if (git_odb_exists(odb_tgt, oid))
		return 0;

	if ((ret = git_tree_lookup(&tree, repo_src, oid)) < 0)
		return ret;

	for (i = 0; i < git_tree_entrycount(tree); ++i) {
		if ((entry = git_tree_entry_byindex(tree, i)) == NULL) {
			ret = -1;
			goto cleanup;
		}

		switch (git_tree_entry_type(entry)) {
		case GIT_OBJ_TREE:
			if ((ret = copy_tree(repo_src, odb_src, odb_tgt, git_tree_entry_id(entry), stats)) < 0)
				goto cleanup;
			break;
		case GIT_OBJ_BLOB:
			if ((ret = copy_object(odb_src, odb_tgt, git_tree_entry_id(entry), stats)) < 0)
				goto cleanup;
			break;
		case GIT_OBJ_COMMIT:
			/* A commit inside a tree represents a submodule commit and should be skipped. */
			break;
		default:
			ret = -1;
			goto cleanup;
		}
	}

	ret = copy_object(odb_src, odb_tgt, oid, stats);

cleanup:
	git_tree_free(tree);

	return ret;
}

/* Copy a commit and it's dependencies from src repo to tgt repo if they do not exist in tgt yet. */
static int copy_commit(git_repository *repo_src, git_odb *odb_src, git_odb *odb_tgt, const git_oid *oid, git_transfer_progress *stats)
{
	int ret;
	git_commit *commit;

	if (git_odb_exists(odb_tgt, oid))
		return 0;

	if ((ret = git_commit_lookup(&commit, repo_src, oid)) < 0)
		return ret;

		if ((ret = copy_tree(repo_src, odb_src, odb_tgt, git_commit_tree_oid(commit), stats)) < 0)
			goto cleanup;

	ret = copy_object(odb_src, odb_tgt, oid, stats);

cleanup:
	git_commit_free(commit);

	return ret;
}

/* This current implementation works by recursively reading objects from the
 * src repo and writing to the tgt repo.
 * TODO: When libgit2 supports creating packfiles, consider rewriting this to
 * share the smart protocol implementation used by the http and git transports.
 * Also, this implementation returns the number of commits instead of objects
 * for the members in stats because we do not know the total number of objects
 * to copy ahead of time.
 */
static int local_download_pack(git_transport *transport, git_repository *repo, git_transfer_progress *stats)
{
	int ret = 0;
	unsigned int i = 0;
	git_oid *oid = NULL;

	/* t->repo is the src */
	transport_local *t = (transport_local *) transport;
	git_odb *odb_src = NULL;

	/* repo parameter is the tgt */
	git_odb *odb_tgt = NULL;


	memset(stats, 0, sizeof(*stats));

	if (t->commit_oids_to_fetch.length == 0) {
		/* for some reason no oids are needed.  this should be impossible to hit */
		return 0;
	}

	stats->total_objects = t->commit_oids_to_fetch.length;

	if ((ret = git_repository_odb__weakptr(&odb_src, t->repo)) < 0)
		return ret;

	if ((ret = git_repository_odb__weakptr(&odb_tgt, repo)) < 0)
		return ret;

	/*
	 * Fetch all missing objects recursively by copying them from the odb_src to odb_tgt.
	 * This is ordered all of an objects dependencies are copied (or verified to exist in odb_tgt) before that object itself is copied.
	 */
	git_vector_rforeach(&t->commit_oids_to_fetch, i, oid) {
		++(stats->received_objects);
		 /* abort upon any errors to avoid writing parent objects without their children */
		if ((ret = copy_commit(t->repo, odb_src, odb_tgt, oid, stats)) < 0)
			break;
		++(stats->indexed_objects);
	}

	return ret;
}

static int local_close(git_transport *transport)
{
	transport_local *t = (transport_local *)transport;

	t->parent.connected = 0;
	git_repository_free(t->repo);
	t->repo = NULL;

	return 0;
}

static void local_free(git_transport *transport)
{
	unsigned int i;
	transport_local *t = (transport_local *) transport;
	git_vector *vec = &transport->refs;
	git_pkt_ref *pkt;
	git_oid *o;

	assert(transport);

	git_vector_foreach (vec, i, pkt) {
		git__free(pkt->head.name);
		git__free(pkt);
	}
	git_vector_free(vec);

	vec = &t->commit_oids_to_fetch;
	git_vector_foreach(vec, i, o) {
		git__free(o);
	}
	git_vector_free(vec);

	git__free(t->parent.url);
	git__free(t);
}

/**************
 * Public API *
 **************/

int git_transport_local(git_transport **out)
{
	transport_local *t;

	t = git__malloc(sizeof(transport_local));
	GITERR_CHECK_ALLOC(t);

	memset(t, 0x0, sizeof(transport_local));

	t->parent.own_logic = 1;
	t->parent.connect = local_connect;
	t->parent.negotiate_fetch = local_negotiate_fetch;
	t->parent.download_pack = local_download_pack;
	t->parent.close = local_close;
	t->parent.free = local_free;

	*out = (git_transport *) t;

	return 0;
}

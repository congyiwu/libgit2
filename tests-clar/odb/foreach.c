#include "clar_libgit2.h"
#include "odb.h"
#include "git2/odb_backend.h"
#include "pack.h"

static git_odb *_odb;
static git_repository *_repo;
static int nobj;

void test_odb_foreach__cleanup(void)
{
	git_odb_free(_odb);
	git_repository_free(_repo);
}

static int foreach_cb(git_oid *oid, void *data)
{
	GIT_UNUSED(data);
	GIT_UNUSED(oid);

	nobj++;

	return 0;
}

/*
 * $ git --git-dir tests-clar/resources/testrepo.git count-objects --verbose
 * count: 43
 * size: 3
 * in-pack: 1640
 * packs: 3
 * size-pack: 425
 * prune-packable: 0
 * garbage: 0
 */
void test_odb_foreach__foreach(void)
{
	cl_git_pass(git_repository_open(&_repo, cl_fixture("testrepo.git")));
	git_repository_odb(&_odb, _repo);

	cl_git_pass(git_odb_foreach(_odb, foreach_cb, NULL));
	cl_assert_equal_i(43 + 1640, nobj); /* count + in-pack */
}

void test_odb_foreach__one_pack(void)
{
	git_odb_backend *backend = NULL;

	cl_git_pass(git_odb_new(&_odb));
	cl_git_pass(git_odb_backend_one_pack(&backend, cl_fixture("testrepo.git/objects/pack/pack-a81e489679b7d3418f9ab594bda8ceb37dd4c695.idx")));
	cl_git_pass(git_odb_add_backend(_odb, backend, 1));
	_repo = NULL;

	nobj = 0;
	cl_git_pass(git_odb_foreach(_odb, foreach_cb, NULL));
	cl_assert(nobj == 1628);
}

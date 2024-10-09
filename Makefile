# Include core make support
-include .make/base.mk

-include .make/python.mk

-include .make/oci.mk

MAKE_GIT_HOOKS_DIR := .githooks/

docs-pre-build: docs/src docs/Makefile
	make -C docs/ create-doc
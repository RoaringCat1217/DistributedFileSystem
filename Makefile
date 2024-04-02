export GO111MODULE=on
# folder name of the package of interest and supporting library
PKGNAME = naming storage common

GSONFILE = gson-2.8.6.jar

# where are all the source files for main package and test code
SRCFILES = common/*.java
TESTFILES = test/*.java test/util/*.java $(foreach pkg,$(PKGNAME),test/$(pkg)/*.java)
NAMEING_DIR = naming/
NAMING_BIN = naming
STORAGE_DIR = storage/
STORAGE_BIN = storage
OUT_DIR = ../out/

# javadoc output directory and library url
DOCDIR = doc
DOCLINK = https://docs.oracle.com/en/java/javase/21/docs/api

.PHONY: build final checkpoint clean docs docs-test
.SILENT: build final checkpoint clean docs docs-test

GOBUILD=go build
GOCLEAN=go clean
GOTEST=go test
GOGET=go get

# build go files only
build-go:
	cd $(NAMEING_DIR) && $(GOBUILD) -o $(OUT_DIR)$(NAMING_BIN) -v
	cd ..
	cd $(STORAGE_DIR) && $(GOBUILD) -o $(OUT_DIR)$(STORAGE_BIN) -v

# compile all source files
build:
	javac -cp $(GSONFILE) $(TESTFILES) $(SRCFILES)
	cd $(NAMEING_DIR) && $(GOBUILD) -o $(OUT_DIR)$(NAMING_BIN) -v
	cd ..
	cd $(STORAGE_DIR) && $(GOBUILD) -o $(OUT_DIR)$(STORAGE_BIN) -v

# run tests
final: build
	java -cp .:$(GSONFILE) test.Lab3FinalTests

checkpoint: build
	java -cp .:$(GSONFILE) test.Lab3CheckpointTests
    
# delete all class files and docs, leaving only source
clean:
	rm -rf $(SRCFILES:.java=.class) $(TESTFILES:.java=.class) $(DOCDIR) $(DOCDIR)-test
	rm -rf out


# generate documentation for the package of interest
docs:
	javadoc -cp .:$(GSONFILE) -private -link $(DOCLINK) -d $(DOCDIR) $(PKGNAME)
 
# generate documentation for the test suite
docs-test:
	javadoc -cp .:$(GSONFILE) -private -link $(DOCLINK) -d $(DOCDIR)-test test test.util $(foreach pkg,$(PKGNAME),test.$(pkg))
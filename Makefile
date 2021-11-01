REPO_NAME='coinbase-spark'
PREFIX='barkingbad'

all: push

container: image

image:
	docker build -t $(PREFIX)/$(REPO_NAME) . # Build new image and automatically tag it as latest

run:
	docker run --rm --network="host" $(PREFIX)/$(REPO_NAME):latest start-notebook.sh --NotebookApp.token=''

push: image
	docker push $(PREFIX)/$(REPO_NAME) # Push image tagged as latest to repository

clean:


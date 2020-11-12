REPO_NAME='openaq-streamer'
PREFIX='balis'

all: push

container: image

image:
	docker build -t $(PREFIX)/$(REPO_NAME) . # Build new image and automatically tag it as latest

push: image
	docker push $(PREFIX)/$(REPO_NAME) # Push image tagged as latest to repository

clean:

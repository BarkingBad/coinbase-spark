REPO_NAME='openaq-streamer'
PREFIX='balis'

all: push

container: image

image:
	docker build -t $(PREFIX)/$(REPO_NAME) . # Build new image and automatically tag it as latest

run:
	docker run --rm -p 127.0.0.1:8888:8888/tcp $(PREFIX)/$(REPO_NAME):latest

push: image
	docker push $(PREFIX)/$(REPO_NAME) # Push image tagged as latest to repository

clean:


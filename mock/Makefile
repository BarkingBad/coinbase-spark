REPO_NAME=ws-streamer
PREFIX=barkingbad

all: push

container: image

image:
	docker build -t  $(PREFIX)/$(REPO_NAME) . # Build new image and automatically tag it as latest

run:
	docker run -p 8025:8025 -it $(PREFIX)/$(REPO_NAME):latest Mock.scala

push: image
	docker push $(PREFIX)/$(REPO_NAME) # Push image tagged as latest to repository

clean:

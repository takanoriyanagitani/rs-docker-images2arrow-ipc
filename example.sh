#!/bin/sh

sockpath="${ENV_DOCKER_SOCK_PATH:-/var/run/docker.sock}"

echo example 1 using arrow-cat
./rs-docker-images2arrow-ipc \
	--docker-sock-path "${sockpath}" \
	--docker-conn-timeout 10 |
	arrow-cat |
	tail -3

echo
echo example 2 using sql
./rs-docker-images2arrow-ipc \
	--docker-sock-path "${sockpath}" \
	--docker-conn-timeout 10 |
	rs-ipc-stream2df \
	--max-rows 1024 \
	--tabname 'docker_images' \
	--sql "
		WITH simplified AS (
			SELECT
				id,
				repo_tags[1] AS repo_tag,
				created,
				size,
				containers
			FROM docker_images
		)
		SELECT
			*
		FROM simplified
		WHERE repo_tag NOT LIKE 'f%'
		ORDER BY size DESC
		LIMIT 10
	" |
	rs-arrow-ipc-stream-cat

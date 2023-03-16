dep_upgrade_list:
	go list -u -m all

dep_upgrade_all:
	go get -t -u ./...

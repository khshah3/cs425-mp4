all:
	go build myks.go
	go build benchmark.go


clean:
	rm myks benchmark test client

mw:
	gnome-terminal -x ./myks -l="5555"
	sleep 1
	gnome-terminal -x ./myks -l="5556" -g="127.0.1.1:5555"
	gnome-terminal -x ./myks -l="5557" -g="127.0.1.1:5555"

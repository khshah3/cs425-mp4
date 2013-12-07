
=======
cs425-mp3
=========

Chord-like distributed hashtable

Setup
-------
Once downloaded to run the program typ
-make : this should create an executable called myks
- type ./myks, this will create the first server
- on other computers, join by typing ./myks with flags
- -l='port'
- -g='firstcomputerhostport'

Modules
-------


_ring_: Handles distributing keys/machines on the ring

_usertable_: Machines, addresses, locations on ring. Allows updating a machine's
          location. May be redundant if we put all this functionality in 'ring'
          instead.
_data_ : Handles data storage , handling and marshalling as well group member storage



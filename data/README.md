# Date locale ale Brokerului

La prima pornire, Brokerul folosește `data/broker-state.json` pentru mesaje, abonamente și stările livrărilor. Fișierul JSON este ignorat de Git; nu îl trimiteți pe GitHub. Dacă există o stare veche în `bin`, Brokerul o copiază aici la prima pornire, fără să o șteargă sau să suprascrie o stare deja existentă.

Calea poate fi schimbată prin variabila `BROKER_STATE_FILE`.

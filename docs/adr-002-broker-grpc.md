# ADR-002 — gRPC pentru publicarea în Broker

**Context.** Laboratorul 2 cere un RPC cu contract versionat și tratarea cazului în care răspunsul se pierde după operația la distanță. Sistemul existent livrează către Consumer prin TCP și păstrează starea într-un fișier JSON.

**Decizie.** Brokerul expune un serviciu gRPC v1 pentru publicare, verificarea rezultatului, abonarea Consumerului și confirmarea livrării. `message_id` este cheia de deduplicare persistentă. Contractul `.proto` este păstrat în directorul Broker. Un răspuns pierdut se rezolvă prin interogarea statusului și, doar dacă este necesar, retry cu aceeași cheie.

**Alternative.** HTTP/JSON ar fi mai ușor de apelat manual, dar gRPC oferă contractul explicit `.proto` și suport nativ pentru deadline/cancellation. Un retry orb fără deduplicare ar putea crea publicații multiple.

**Consecințe.** Brokerul are acum un port gRPC suplimentar și dependențe NuGet. Testele de integrare verifică pierderea răspunsului. Acceptarea este idempotentă, dar nu există promisiune `exactly once` pentru efectele Consumerului.

**Limită cunoscută.** Fișierul JSON este adecvat pentru laborator și un singur proces Broker; nu este o bază de date pentru mai multe instanțe concurente sau pentru durabilitate de producție. Transportul gRPC este local fără TLS în această configurație.

# Laboratorul 2 — Broker (partea de integrare)

## Contract și delimitarea responsabilităților

Contractul gRPC este [`broker_v1.proto`](../DistributedApp/DistributedApp.Broker/Protos/broker_v1.proto), pachetul `distributedapp.broker.v1`, `schema_version = "1.0"`. Producerul folosește clientul generat din acest fișier pentru publicare; Consumerul continuă să folosească protocolul TCP pentru abonare și confirmarea livrărilor. Schimbările Producerului sunt deja pe `feature/broker`, iar schimbările Consumerului se integrează separat de pe branchul Valeriei. Orice schimbare incompatibilă cere o versiune nouă a serviciului, nu reutilizarea numerelor de câmp.

```text
Producer --gRPC Publish(request_id, message_id, correlation_id)--> Broker
Producer <--gRPC Accepted / DeadlineExceeded--------------------- Broker
Producer --gRPC GetPublishStatus(message_id)---------------------> Broker
Broker   --TCP Delivery------------------------------------------> Consumer
Broker   <--TCP ACK/NACK------------------------------------------ Consumer
```

`request_id` identifică apelul, `message_id` este cheia stabilă de idempotency și `correlation_id` leagă întregul flux. Producerul trebuie să păstreze aceleași valori la retry, nu să genereze un mesaj nou. `occurred_at_utc` trebuie să conțină un moment UTC. Brokerul validează versiunea, tipul, identificatorii și câmpurile necesare înainte de efect.

## Rezultat ambiguu și fallback

Brokerul salvează publicația înainte să returneze `Accepted`. Dacă apelantul primește `DeadlineExceeded` sau pierde conexiunea, **nu poate presupune că operația a eșuat**. Consultă `GetPublishStatus(message_id)`; dacă răspunsul este `Accepted`, se oprește. Dacă este `NotFound`, poate relua `Publish` cu **același** `message_id`. Repetarea identică produce `duplicate = true` fără încă o publicație. Reutilizarea ID-ului cu alt conținut produce `AlreadyExists`. Retry-ul trebuie să fie limitat și să respecte deadline-ul apelantului; politica de retry aparține Producerului.

Acest mecanism garantează deduplicarea **acceptării în Broker** pentru un `message_id` păstrat în fișierul de stare. Nu pretinde `exactly once` pentru efectele executate de Consumer. Brokerul TCP existent poate retransmite, iar Consumerul trebuie să fie idempotent pentru propriul efect.

## Configurare și verificare

Implicit: gRPC pe `127.0.0.1:5001` (HTTP/2) și TCP pe portul `5000`. Variabile opționale: `BROKER_GRPC_PORT`, `BROKER_TCP_PORT`, `BROKER_STATE_FILE`. Nu există chei secrete în repository. Starea se află implicit în `data/broker-state.json` la rădăcina clonei și este ignorată de Git. Dacă există numai fișierul vechi din `bin`, acesta este copiat o singură dată la prima pornire. Fișierul de stare este creat la prima schimbare salvată; dacă este corupt, Brokerul nu pornește cu o stare goală care ar anula deduplicarea. Endpoint-ul `CheckHealth` este ieftin și nu modifică starea.

Din rădăcina clonei `PAD`:

```powershell
dotnet build .\DistributedApp\DistributedApp.sln
dotnet test .\tests\DistributedApp.Broker.Lab2.Tests\DistributedApp.Broker.Lab2.Tests.csproj
dotnet run --project .\DistributedApp\DistributedApp.Broker\DistributedApp.Broker.csproj
```

Testele Brokerului sunt într-un proiect separat de `.sln`, dar rămân în repository conform cerinței de predare. Clientul Producer și testele Consumerului sunt responsabilități separate; rularea acestui proiect verifică doar partea Brokerului.

## Dovada scenariului critic

Testul `LostReplyIsResolvedByStatusAndRetryDoesNotDuplicate` pornește Brokerul cu fișier temporar, trimite `Publish` cu antetul **numai pentru demonstrație** `x-simulate-lost-response: true` și un deadline scurt. Brokerul persistă mesajul, apoi întârzie deliberat răspunsul până când apelantul primește eroare de deadline. `GetPublishStatus` întoarce `Accepted`; retry-ul identic întoarce `duplicate = true`, iar fișierul conține o singură publicație. Testul `LostReplyCanBeResolvedByStatusAfterBrokerRestart` demonstrează că starea rămâne disponibilă după repornire.

| Etapă | Stare / observație verificabilă |
| --- | --- |
| Inițial | ID-ul nou nu există în starea Brokerului. |
| Injectare | `Publish` salvează mesajul, dar antetul de test blochează răspunsul până la deadline. |
| Observație la Producer | Apelul primește `DeadlineExceeded`; rezultatul operației rămâne ambiguu. |
| Recuperare | `GetPublishStatus` găsește mesajul cu status `Accepted`. |
| Retry de verificare | Același `message_id` întoarce `duplicate = true`. |
| Final | Fișierul de stare conține exact o publicație pentru acel ID. |

Comanda de test de mai sus a fost executată pe `feature/broker`: **8 teste trecute, 0 eșuate**. Nu se folosește un fișier de stare din `data/` la teste; fiecare scenariu are o cale temporară separată. Un test separat verifică faptul că reutilizarea unui `message_id` cu alt conținut produce `AlreadyExists` și nu adaugă altă publicație.

Limite: nu există tranzacție distribuită cu Consumerul; `GetPublishStatus` arată acceptarea în Broker, nu finalizarea livrării; portul gRPC este configurat pentru dezvoltare locală fără TLS. În producție ar trebui adăugate TLS, autentificare și o stocare tranzacțională robustă.

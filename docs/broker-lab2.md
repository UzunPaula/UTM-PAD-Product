# Laboratorul 2 — Broker (partea de integrare)

## Contract și delimitarea responsabilităților

Contractul gRPC este [`broker.proto`](../DistributedApp/DistributedApp.Broker/Protos/broker.proto), pachetul `distributedapp.broker.v1`, `schema_version = "1.0"`. Producerul folosește `Publish` și `GetPublishStatus`. Brokerul oferă acum și `SubscribeConsumer` (stream de confirmare și livrări) și `AcknowledgeDelivery` (ACK/NACK) pentru clientul Consumer gRPC. Clientul Consumer existent încă folosește TCP până când este adaptat pe branchul său; listenerul TCP rămâne compatibil. Orice schimbare incompatibilă cere o versiune nouă a serviciului, nu reutilizarea numerelor de câmp.

```text
Producer --gRPC Publish(request_id, message_id, correlation_id)--> Broker
Producer <--gRPC Accepted / DeadlineExceeded--------------------- Broker
Producer --gRPC GetPublishStatus(message_id)---------------------> Broker
Consumer --gRPC SubscribeConsumer(consumer_id, topic)------------> Broker
Consumer <--gRPC Subscribed(session_id), apoi Delivery stream----- Broker
Consumer --gRPC AcknowledgeDelivery(ACK/NACK, session_id)--------> Broker
```

La `SubscribeConsumer`, primul eveniment din stream este `subscribed`, cu `session_id`; numai după el urmează evenimente `delivery`. ID-ul Consumerului trebuie să fie stabil, iar o a doua conexiune activă cu același ID primește eroarea gRPC `AlreadyExists`. Consumerul păstrează `session_id` și îl trimite în fiecare `AcknowledgeDelivery`, împreună cu `message_id` și `correlation_id`. ACK-ul repetat pentru o livrare deja confirmată nu creează încă o confirmare; NACK-ul programează relivrarea conform politicii Brokerului. O sesiune veche nu poate confirma livrările sesiunii active. La închiderea stream-ului, Brokerul eliberează conexiunea; Consumerul poate reconecta cu același ID. Pentru clienții TCP vechi, `Subscribe` continuă să primească `Ack`/`Nack` înainte de livrare.

`request_id` identifică apelul, `message_id` este cheia stabilă de idempotency și `correlation_id` leagă întregul flux. Producerul trebuie să păstreze aceleași valori la retry, nu să genereze un mesaj nou. `occurred_at_utc` trebuie să conțină un moment UTC. Brokerul validează versiunea, tipul, identificatorii și câmpurile necesare înainte de efect.

## Rezultat ambiguu și fallback

Brokerul salvează publicația înainte să returneze `Accepted`. Dacă apelantul primește `DeadlineExceeded` sau pierde conexiunea, **nu poate presupune că operația a eșuat**. Consultă `GetPublishStatus(message_id)`; dacă răspunsul este `Accepted`, se oprește. Dacă este `NotFound`, poate relua `Publish` cu **același** `message_id`. Repetarea identică produce `duplicate = true` fără încă o publicație. Reutilizarea ID-ului cu alt conținut produce `AlreadyExists`. Retry-ul trebuie să fie limitat și să respecte deadline-ul apelantului; politica de retry aparține Producerului.

Acest mecanism garantează deduplicarea **acceptării în Broker** pentru un `message_id` păstrat în fișierul de stare. Nu pretinde `exactly once` pentru efectele executate de Consumer. Brokerul TCP existent poate retransmite, iar Consumerul trebuie să fie idempotent pentru propriul efect.

## Configurare și verificare

Implicit: gRPC pe `127.0.0.1:5001` (HTTP/2) pentru Producer și noul Consumer, respectiv TCP pe portul `5000` pentru compatibilitate. Variabile opționale: `BROKER_GRPC_PORT`, `BROKER_TCP_PORT`, `BROKER_STATE_FILE`. Nu există chei secrete în repository. Starea se află implicit în `data/broker-state.json` la rădăcina clonei și este ignorată de Git. Dacă există numai fișierul vechi din `bin`, acesta este copiat o singură dată la prima pornire. Fișierul de stare este creat la prima schimbare salvată; dacă este corupt, Brokerul nu pornește cu o stare goală care ar anula deduplicarea. Endpoint-ul `CheckHealth` este ieftin și nu modifică starea.

Din rădăcina clonei `PAD`:

```powershell
dotnet build .\DistributedApp\DistributedApp.sln
dotnet test .\tests\DistributedApp.Broker.Lab2.Tests\DistributedApp.Broker.Lab2.Tests.csproj
dotnet run --project .\DistributedApp\DistributedApp.Broker\DistributedApp.Broker.csproj
```

Proiectul de teste Broker este inclus în soluție, deci `dotnet test .\DistributedApp\DistributedApp.sln` rulează și testele Consumerului. Testele de integrare pornesc un proces Broker cu porturi și fișiere de stare temporare; nu modifică starea demonstrației din `data/`.

Pentru prezentare, corespondența dintre cerință și dovadă este:

| Cerință | Dovadă în teste |
| --- | --- |
| Contract și compatibilitate | `BrokerPublishingTests`: numerele câmpurilor rămân stabile, iar un câmp Protobuf nou nu împiedică citirea de către versiunea veche. |
| Deadline, rezultat ambiguu și fallback | `HttpOrderEndpointResolvesLostGrpcReply` și `LostReplyIsResolvedByStatusAndRetryDoesNotDuplicate`: răspuns pierdut după salvare, verificare de status, o singură publicație. |
| Retry și idempotency | `NotFoundFallbackRetriesWithTheSameMessageId` și testele de publicare duplicată: același ID este refolosit; nu apare o a doua publicație. |
| Cancellation | `CancellationBeforePublishDoesNotCreateARecord` și `CancellingAfterSaveDoesNotUndoTheRemoteOperation`: anularea apelului nu înseamnă automat anularea efectului deja salvat. |
| gRPC Consumer și recuperare | `BrokerConsumerGrpcTests`: abonare confirmată, livrare, ACK/NACK, refuzul conexiunii duplicate și relivrare după ACK pierdut. Testul folosește un client gRPC de test, nu executabilul Valeriei. |
| Health fără efect secundar | `HealthCheckDoesNotCreateBrokerState`: răspunsul de sănătate nu creează fișierul de stare. |

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

Testele Brokerului verifică și fluxul gRPC Consumer: confirmarea abonării înainte de livrare, ACK repetat fără efect dublu, NACK urmat de relivrare, sesiune greșită și abonare duplicată. Nu se folosește un fișier de stare din `data/` la teste; fiecare scenariu are o cale temporară separată. Testele TCP continuă să verifice abonarea clienților existenți.

Testele de integrare suplimentare verifică: clientul Producer real și endpointul său HTTP `/api/orders` cu răspuns gRPC pierdut, recuperat prin `GetPublishStatus` fără o a doua publicație; fallback-ul `NotFound` urmat de retry cu același `message_id`; Broker indisponibil cu rezultat `Unknown` în timp limitat; anularea înainte de publicare; anularea după salvare fără anularea efectului; un câmp Protobuf nou necunoscut cititorului vechi; un `CheckHealth` care nu scrie stare; și ACK pierdut urmat de relivrare. Ramura `NotFound` folosește un serviciu gRPC de test care răspunde întâi `Unavailable`, apoi `NotFound`; celelalte teste pornesc Brokerul real. În scenariul ACK pierdut, clientul gRPC din test folosește `ProcessedMessageStore` real al Consumerului și dovedește că efectul se salvează o singură dată.

Clientul executabil al Consumerului încă folosește TCP. Testul gRPC verifică Brokerul și stocarea Consumerului, dar **nu** înlocuiește testul final cu executabilul Consumer după ce partea lui gRPC este implementată pe branchul Consumerului.

Limite: nu există tranzacție distribuită cu Consumerul; `GetPublishStatus` arată acceptarea în Broker, nu finalizarea livrării; portul gRPC este configurat pentru dezvoltare locală fără TLS. În producție ar trebui adăugate TLS, autentificare și o stocare tranzacțională robustă.

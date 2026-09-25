const api = {
    status: "/api/status",
    orders: "/api/orders",
    deadLetters: "/api/dead-letters"
};

const elements = {
    form: document.querySelector("#orderForm"),
    orderId: document.querySelector("#orderId"),
    product: document.querySelector("#product"),
    quantity: document.querySelector("#quantity"),
    submitButton: document.querySelector("#submitButton"),
    refreshButton: document.querySelector("#refreshButton"),
    result: document.querySelector("#submissionResult"),
    brokerState: document.querySelector("#brokerState"),
    consumerState: document.querySelector("#consumerState"),
    systemBadge: document.querySelector("#systemBadge"),
    systemBadgeText: document.querySelector("#systemBadgeText"),
    pendingCount: document.querySelector("#pendingCount"),
    inFlightCount: document.querySelector("#inFlightCount"),
    acknowledgedCount: document.querySelector("#acknowledgedCount"),
    deadLetterCount: document.querySelector("#deadLetterCount"),
    deadLetterListCount: document.querySelector("#deadLetterListCount"),
    deadLetterBody: document.querySelector("#deadLetterBody"),
    lastUpdated: document.querySelector("#lastUpdated"),
    historyBody: document.querySelector("#historyBody"),
    historyCount: document.querySelector("#historyCount")
};

const history = [];
let statusRefreshInProgress = false;

function createOrderId() {
    const suffix = crypto.randomUUID().slice(0, 8).toUpperCase();
    return `ORD-${suffix}`;
}

function setFieldError(fieldName, message) {
    const input = elements[fieldName];
    const error = document.querySelector(`[data-error-for="${fieldName}"]`);
    input.classList.toggle("invalid", Boolean(message));
    input.setAttribute("aria-invalid", String(Boolean(message)));
    error.textContent = message;
}

function validateOrder(order) {
    const errors = {
        orderId: order.orderId ? "" : "ID-ul comenzii este obligatoriu.",
        product: order.product ? "" : "Produsul este obligatoriu.",
        quantity: Number.isInteger(order.quantity) && order.quantity >= 1 && order.quantity <= 1000
            ? ""
            : "Cantitatea trebuie să fie între 1 și 1000."
    };

    Object.entries(errors).forEach(([field, message]) => setFieldError(field, message));
    return !Object.values(errors).some(Boolean);
}

function setComponentState(element, isOnline) {
    const state = isOnline === true
        ? { label: "Online", className: "online" }
        : isOnline === false
            ? { label: "Offline", className: "offline" }
            : { label: "Necunoscut", className: "unknown" };
    element.textContent = state.label;
    element.className = `state-pill ${state.className}`;
}

function renderSystemState(brokerState, consumerState) {
    setComponentState(elements.brokerState, brokerState);
    setComponentState(elements.consumerState, consumerState);

    const bothOnline = brokerState === true && consumerState === true;
    const brokerOnly = brokerState === true;
    elements.systemBadge.className = `system-badge ${bothOnline ? "online" : brokerOnly ? "degraded" : "offline"}`;
    elements.systemBadgeText.textContent = bothOnline
        ? "Sistem operațional"
        : brokerOnly ? "Broker disponibil" : "Sistem offline";
}

function renderDeadLetters(deadLetters) {
    const entries = Array.isArray(deadLetters) ? deadLetters : [];
    elements.deadLetterListCount.textContent =
        `${entries.length} ${entries.length === 1 ? "mesaj" : "mesaje"}`;
    elements.deadLetterBody.innerHTML = "";

    if (entries.length === 0) {
        const row = document.createElement("tr");
        row.className = "empty-row";
        row.innerHTML = '<td colspan="6">Nu există mesaje în dead-letter queue.</td>';
        elements.deadLetterBody.append(row);
        return;
    }

    entries.forEach(entry => {
        const row = document.createElement("tr");
        const values = [
            entry.messageId,
            entry.topic,
            entry.retryCount,
            entry.reason,
            entry.deadLetteredAtUtc
                ? new Date(entry.deadLetteredAtUtc).toLocaleString("ro-RO")
                : "Necunoscut"
        ];

        values.forEach((value, index) => {
            const cell = document.createElement("td");
            cell.textContent = value ?? "—";
            if (index === 0) {
                cell.className = "message-id";
                cell.title = value ?? "";
            } else if (index === 3) {
                cell.className = "reason-cell";
            }
            row.append(cell);
        });
        const actionCell = document.createElement("td");
        const redriveButton = document.createElement("button");
        redriveButton.type = "button";
        redriveButton.className = "redrive-button";
        redriveButton.textContent = "Reîncearcă";
        redriveButton.title =
            "Scoate mesajul din DLQ și reintrodu-l în coada normală";
        redriveButton.addEventListener("click", () =>
            redriveDeadLetter(entry.messageId, redriveButton));
        actionCell.append(redriveButton);
        row.append(actionCell);
        elements.deadLetterBody.append(row);
    });
}

function updateHistoryDeliveryStates(status) {
    const acknowledged = new Set(
        (status.recentAcknowledgements || [])
            .map(entry => String(entry.messageId).toLowerCase()));
    const deadLetters = new Set(
        (status.deadLetters || [])
            .map(entry => String(entry.messageId).toLowerCase()));

    history.forEach(item => {
        if (!item.messageId) return;
        const messageId = String(item.messageId).toLowerCase();
        if (deadLetters.has(messageId)) {
            item.state = "dead-letter";
        } else if (acknowledged.has(messageId)) {
            item.state = "delivered";
        } else if (item.state === "dead-letter") {
            item.state = "pending";
        }
    });
    renderHistory();
}

async function redriveDeadLetter(messageId, button) {
    button.disabled = true;
    button.textContent = "Se mută…";

    try {
        const response = await fetch(
            api.deadLetters + "/" + encodeURIComponent(messageId) + "/redrive",
            {
                method: "POST",
                headers: { Accept: "application/json" }
            });
        const result = await response.json().catch(() => ({}));

        if (!response.ok || result.redriven !== true) {
            throw new Error(
                result.reason
                || "Mesajul nu a putut fi reintrodus în coadă.");
        }

        const historyItem = history.find(item =>
            String(item.messageId).toLowerCase()
                === String(messageId).toLowerCase());
        if (historyItem) {
            historyItem.state = "pending";
            renderHistory();
        }

        showResult(
            true,
            "Mesajul a fost scos din DLQ.",
            "Mesajul " + messageId
                + " a fost reintrodus în coada orders și este preluat de threadul Brokerului.");
        await refreshStatus();
    } catch (error) {
        showResult(
            false,
            "Redrive eșuat.",
            error.message || "Mesajul nu a putut fi reintrodus.");
    } finally {
        button.disabled = false;
        button.textContent = "Reîncearcă";
    }
}
async function refreshStatus() {
    if (statusRefreshInProgress) return;
    statusRefreshInProgress = true;
    elements.refreshButton.disabled = true;
    try {
        const response = await fetch(api.status, { headers: { Accept: "application/json" } });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        const status = await response.json();
        renderSystemState(status.brokerConnected, status.consumerConnected);
        elements.pendingCount.textContent = Number.isInteger(status.pendingMessages) ? status.pendingMessages : "—";
        elements.inFlightCount.textContent = Number.isInteger(status.inFlightMessages) ? status.inFlightMessages : "—";
        elements.acknowledgedCount.textContent = Number.isInteger(status.acknowledgedMessages) ? status.acknowledgedMessages : "—";
        elements.deadLetterCount.textContent = Number.isInteger(status.deadLetterMessages) ? status.deadLetterMessages : "—";
        renderDeadLetters(status.deadLetters);
        updateHistoryDeliveryStates(status);
        elements.lastUpdated.textContent = new Date().toLocaleTimeString("ro-RO", { hour: "2-digit", minute: "2-digit" });
    } catch {
        renderSystemState(false, false);
        elements.pendingCount.textContent = "—";
        elements.inFlightCount.textContent = "—";
        elements.acknowledgedCount.textContent = "—";
        elements.deadLetterCount.textContent = "—";
        renderDeadLetters([]);
        elements.lastUpdated.textContent = "Backend indisponibil";
    } finally {
        statusRefreshInProgress = false;
        elements.refreshButton.disabled = false;
    }
}

function showResult(success, title, detail) {
    elements.result.hidden = false;
    elements.result.className = `result-card ${success ? "success" : "error"}`;
    elements.result.innerHTML = "";

    const heading = document.createElement("strong");
    const description = document.createElement("span");
    heading.textContent = title;
    description.textContent = detail;
    elements.result.append(heading, description);
}

function renderHistory() {
    elements.historyCount.textContent = `${history.length} ${history.length === 1 ? "comandă" : "comenzi"}`;
    elements.historyBody.innerHTML = "";

    if (history.length === 0) {
        const row = document.createElement("tr");
        row.className = "empty-row";
        row.innerHTML = '<td colspan="5">Nu au fost trimise comenzi în această sesiune.</td>';
        elements.historyBody.append(row);
        return;
    }

    history.forEach(item => {
        const row = document.createElement("tr");
        const values = [item.orderId, item.product, item.quantity];
        values.forEach(value => {
            const cell = document.createElement("td");
            cell.textContent = value;
            row.append(cell);
        });

        const states = {
            published: { label: "Publicat", className: "pending" },
            delivered: { label: "Livrat", className: "success" },
            "dead-letter": { label: "Dead-letter", className: "error" },
            failed: { label: "Respins", className: "error" }
        };
        const state = states[item.state] || states.failed;
        const stateCell = document.createElement("td");
        stateCell.className = `table-state ${state.className}`;
        stateCell.textContent = state.label;
        row.append(stateCell);

        const messageCell = document.createElement("td");
        messageCell.className = "message-id";
        messageCell.title = item.messageId || item.detail;
        messageCell.textContent = item.messageId || item.detail;
        row.append(messageCell);
        elements.historyBody.append(row);
    });
}

async function submitOrder(event) {
    event.preventDefault();
    const order = {
        orderId: elements.orderId.value.trim(),
        product: elements.product.value.trim(),
        quantity: Number(elements.quantity.value)
    };

    if (!validateOrder(order)) return;

    elements.submitButton.disabled = true;
    try {
        const response = await fetch(api.orders, {
            method: "POST",
            headers: { "Content-Type": "application/json", Accept: "application/json" },
            body: JSON.stringify(order)
        });
        const result = await response.json().catch(() => ({}));
        if (!response.ok || result.accepted === false) {
            throw new Error(result.reason || `Cererea a fost respinsă (HTTP ${response.status}).`);
        }

        const detail = `messageId: ${result.messageId || "necomunicat"} · correlationId: ${result.correlationId || "necomunicat"}`;
        showResult(true, "Comanda a fost acceptată de Broker.", detail);
        history.unshift({
            ...order,
            state: "published",
            messageId: result.messageId || "Acceptat"
        });
        elements.form.reset();
        elements.orderId.value = createOrderId();
        elements.quantity.value = 1;
        await refreshStatus();
    } catch (error) {
        const detail = error instanceof Error ? error.message : "Backendul nu este disponibil.";
        showResult(false, "Comanda nu a fost trimisă.", detail);
        history.unshift({ ...order, state: "failed", detail });
    } finally {
        elements.submitButton.disabled = false;
        renderHistory();
    }
}

document.querySelector("#generateOrderId").addEventListener("click", () => {
    elements.orderId.value = createOrderId();
    setFieldError("orderId", "");
});
elements.refreshButton.addEventListener("click", refreshStatus);
elements.form.addEventListener("submit", submitOrder);
elements.orderId.value = createOrderId();
renderHistory();
refreshStatus();
window.setInterval(refreshStatus, 500);

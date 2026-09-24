const api = {
    status: "/api/status",
    orders: "/api/orders"
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
    deadLetterCount: document.querySelector("#deadLetterCount"),
    lastUpdated: document.querySelector("#lastUpdated"),
    historyBody: document.querySelector("#historyBody"),
    historyCount: document.querySelector("#historyCount")
};

const history = [];

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
    element.textContent = isOnline ? "Online" : "Offline";
    element.className = `state-pill ${isOnline ? "online" : "offline"}`;
}

function renderSystemState(brokerOnline, consumerOnline) {
    setComponentState(elements.brokerState, brokerOnline);
    setComponentState(elements.consumerState, consumerOnline);

    const bothOnline = brokerOnline && consumerOnline;
    const oneOnline = brokerOnline || consumerOnline;
    elements.systemBadge.className = `system-badge ${bothOnline ? "online" : oneOnline ? "degraded" : "offline"}`;
    elements.systemBadgeText.textContent = bothOnline
        ? "Sistem operațional"
        : oneOnline ? "Sistem parțial disponibil" : "Sistem offline";
}

async function refreshStatus() {
    elements.refreshButton.disabled = true;
    try {
        const response = await fetch(api.status, { headers: { Accept: "application/json" } });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);

        const status = await response.json();
        renderSystemState(Boolean(status.brokerConnected), Boolean(status.consumerConnected));
        elements.pendingCount.textContent = Number.isInteger(status.pendingMessages) ? status.pendingMessages : "—";
        elements.deadLetterCount.textContent = Number.isInteger(status.deadLetterMessages) ? status.deadLetterMessages : "—";
        elements.lastUpdated.textContent = new Date().toLocaleTimeString("ro-RO", { hour: "2-digit", minute: "2-digit" });
    } catch {
        renderSystemState(false, false);
        elements.pendingCount.textContent = "—";
        elements.deadLetterCount.textContent = "—";
        elements.lastUpdated.textContent = "Backend indisponibil";
    } finally {
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

        const stateCell = document.createElement("td");
        stateCell.className = `table-state ${item.success ? "success" : "error"}`;
        stateCell.textContent = item.success ? "Acceptat" : "Respins";
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
        history.unshift({ ...order, success: true, messageId: result.messageId || "Acceptat" });
        elements.form.reset();
        elements.orderId.value = createOrderId();
        elements.quantity.value = 1;
        await refreshStatus();
    } catch (error) {
        const detail = error instanceof Error ? error.message : "Backendul nu este disponibil.";
        showResult(false, "Comanda nu a fost trimisă.", detail);
        history.unshift({ ...order, success: false, detail });
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

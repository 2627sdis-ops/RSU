const LONGITUD_CARRETERA_M = 5000.0;
let historialSNS = [];

function escaparHTML(valor) {
    return String(valor ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

function renderizarHistorialSNS() {
    const lista = document.getElementById('lista-historial-sns');
    const vacio = document.getElementById('historial-sns-vacio');

    if (!historialSNS.length) {
        lista.innerHTML = '';
        vacio.hidden = false;
        return;
    }

    vacio.hidden = true;
    lista.innerHTML = historialSNS.map(alerta => {
        const evento = alerta.evento || {};
        const tipo = alerta.alert_type || evento.event_type || 'alerta';
        const vehiculo = evento.source_id || 'Cloud';
        const descripcion = alerta.descripcion || 'Sin descripción';
        const hora = alerta.timestamp || '';

        return `
            <li class="item-historial-sns">
                <span class="tipo-historial-sns">${escaparHTML(tipo)}</span>
                <span>${escaparHTML(descripcion)} · Vehículo: <strong>${escaparHTML(vehiculo)}</strong></span>
                <time class="hora-historial-sns">${escaparHTML(hora)}</time>
            </li>
        `;
    }).join('');
}

async function actualizarHistorialSNS() {
    try {
        const respuesta = await fetch('/api/alertas');
        if (!respuesta.ok) {
            throw new Error(`HTTP ${respuesta.status}`);
        }

        const datos = await respuesta.json();
        historialSNS = datos.alertas || [];
        renderizarHistorialSNS();
    } catch (error) {
        console.error('Error consultando el histórico SNS:', error);
    }
}

const ESTADOS = {
    alerta: {
        clase: 'estado-alerta',
        etiqueta: 'Alerta',
        color: '#c05a4a'
    },
    proximidad: {
        clase: 'estado-proximidad',
        etiqueta: 'Proximidad',
        color: '#d99a3d'
    },
    normal: {
        clase: 'estado-ok',
        etiqueta: 'Normal',
        color: '#6f9c3c'
    }
};

// Devuelve la hora de la alerta en milisegundos, o null si no se puede interpretar.
function marcaDeTiempo(alerta) {
    const t = Date.parse(alerta.timestamp);
    return Number.isFinite(t) ? t : null;
}

// De todas las alertas recibidas, se queda solo con la última de cada coche.
// Si los timestamps se pueden comparar, gana el más reciente;
// si no, gana la que aparece más tarde en la lista.
function ultimaAlertaPorCoche(alertas) {
    const ultimas = new Map();

    alertas.forEach((alerta, indice) => {
        const id = alerta.source_id;
        if (id === null || id === undefined) {
            return;
        }

        const actual = ultimas.get(id);
        if (!actual) {
            ultimas.set(id, { alerta, indice });
            return;
        }

        const tNueva = marcaDeTiempo(alerta);
        const tActual = marcaDeTiempo(actual.alerta);
        const esMasReciente =
            tNueva !== null && tActual !== null && tNueva !== tActual
                ? tNueva > tActual
                : indice > actual.indice;

        if (esMasReciente) {
            ultimas.set(id, { alerta, indice });
        }
    });

    return Array.from(ultimas.values(), entrada => entrada.alerta);
}

async function actualizarSimulacion() {
    try {
        const res = await fetch('/api/data');
        const data = await res.json();

        const coches = data.coches || {};
        const alertasCloud = data.alertas_cloud || [];

        const carrilIda = document.getElementById('carril-ida');
        const carrilVuelta = document.getElementById('carril-vuelta');
        const panelAvisos = document.getElementById('panel-avisos');
        const listaAvisos = document.getElementById('lista-avisos');

        // Avisos recibidos en Cloud
        if (alertasCloud.length > 0) {
            panelAvisos.style.display = 'block';

            listaAvisos.innerHTML = alertasCloud.map(alerta => `
                <li>
                    <strong>${alerta.event_type}</strong>
                    · Vehículo: <strong>${alerta.source_id}</strong>
                    · RSU: <strong>${alerta.rsu_id}</strong>
                    ${alerta.x !== null && alerta.x !== undefined
                        ? `· Posición del evento: <strong>${alerta.x} m</strong>`
                        : ''}
                    <span class="aviso-tipo">
                        ${alerta.descripcion}
                        · ${alerta.timestamp}
                    </span>
                </li>
            `).join('');
        } else {
            panelAvisos.style.display = 'none';
            listaAvisos.innerHTML = '';
        }

        // Limpiar visualización anterior
        carrilIda.querySelectorAll('.coche').forEach(el => el.remove());
        carrilVuelta.querySelectorAll('.coche').forEach(el => el.remove());

        // Un único coche por vehículo, colocado según su última alerta.
        const ultimasAlertas = ultimaAlertaPorCoche(alertasCloud);
        const idsConAlerta = new Set();

        ultimasAlertas.forEach(alerta => {
            const posicion = Number(alerta.x);

            if (alerta.x === null || alerta.x === undefined || !Number.isFinite(posicion)) {
                return;
            }

            idsConAlerta.add(alerta.source_id);

            const porcentaje = Math.min(
                100,
                Math.max(0, (posicion / LONGITUD_CARRETERA_M) * 100)
            );

            const cocheEvento = document.createElement('div');
            cocheEvento.className = 'coche alerta';
            cocheEvento.style.left = `${porcentaje}%`;
            cocheEvento.textContent = String(alerta.source_id)
                .replace('veh-', '')
                .replace('Coche_', '');

            cocheEvento.title =
                `${alerta.event_type}: ${alerta.descripcion} ` +
                `(${posicion} m, ${alerta.timestamp})`;

            const carril = alerta.direction === 'vuelta'
                ? carrilVuelta
                : carrilIda;

            carril.appendChild(cocheEvento);
        });

        // Coches en movimiento que no tienen alerta (para no duplicar los que ya se han dibujado).
        Object.values(coches).forEach(coche => {
            if (idsConAlerta.has(coche.source_id)) {
                return;
            }

            const porcentaje = Math.min(100, Math.max(0, (coche.x / LONGITUD_CARRETERA_M) * 100));

            let estado = ESTADOS.normal;
            if (coche.hazard) {
                estado = ESTADOS.alerta;
            } else if (coche.alerta_proximidad) {
                estado = ESTADOS.proximidad;
            }

            const cocheEl = document.createElement('div');
            cocheEl.className = `coche ${estado.clase.replace('estado-', '')}`;
            cocheEl.style.left = `${porcentaje}%`;
            cocheEl.textContent = String(coche.source_id).replace('veh-', '');

            if (coche.direction === 'ida') {
                carrilIda.appendChild(cocheEl);
            } else {
                carrilVuelta.appendChild(cocheEl);
            }
        });

    } catch (err) {
        console.error('Error consultando la API:', err);
    }
}

actualizarSimulacion();
setInterval(actualizarSimulacion, 1000);
actualizarHistorialSNS();
setInterval(actualizarHistorialSNS, 5000);


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

async function actualizarSimulacion() {
    try {
        const res = await fetch('/api/data');
        const data = await res.json();

        //const coches = data.coches;
        //const alertasProximidad = data.alertas_proximidad;
        const coches = data.coches || {};
        const alertasCloud = data.alertas_cloud || [];
        ////////////////////////////////////////////

        const carrilIda = document.getElementById('carril-ida');
        const carrilVuelta = document.getElementById('carril-vuelta');
        const tabla = document.getElementById('tabla-coches');
        const panelAvisos = document.getElementById('panel-avisos');
        const listaAvisos = document.getElementById('lista-avisos');

        // Avisos de proximidad
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
        tabla.innerHTML = '';


        // Marcador del vehículo en la posición registrada al producirse el evento.
        alertasCloud.forEach(alerta => {
            const posicion = Number(alerta.x);
        
            if (!Number.isFinite(posicion)) {
                return;
            }
        
            const porcentaje = Math.min(
                100,
                Math.max(0, (posicion / LONGITUD_CARRETERA_M) * 100)
            );
        
            const cocheEvento = document.createElement('div');
            cocheEvento.className = 'coche alerta';
            cocheEvento.style.left = `${porcentaje}%`;
            cocheEvento.textContent = alerta.source_id
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

        Object.values(coches).forEach(coche => {
            const porcentaje = Math.min(100, Math.max(0, (coche.x / LONGITUD_CARRETERA_M) * 100));

            let estado = ESTADOS.normal;
            if (coche.hazard) {
                estado = ESTADOS.alerta;
            } else if (coche.alerta_proximidad) {
                estado = ESTADOS.proximidad;
            }

            // Coche en la carretera
            const cocheEl = document.createElement('div');
            cocheEl.className = `coche ${estado.clase.replace('estado-', '')}`;
            cocheEl.style.left = `${porcentaje}%`;
            cocheEl.textContent = coche.source_id.replace('veh-', '');

            if (coche.direction === 'ida') {
                carrilIda.appendChild(cocheEl);
            } else {
                carrilVuelta.appendChild(cocheEl);
            }

            // Tarjeta de la flota
            const sentido = coche.direction === 'ida' ? 'Ida' : 'Vuelta';
            const velocidad = Number(coche.speed).toFixed(0);
            const posicion = Number(coche.x).toFixed(0);

            const fila = document.createElement('tr');
            fila.tabIndex = 0;
            fila.innerHTML = `
                <td>
                    <span class="vehiculo-id">${coche.source_id}</span>
                </td>
                <td>
                    <span class="dato-valor">${posicion} m</span>
                </td>
                <td>
                    <span class="sentido-chip">${sentido}</span>
                </td>
                <td>
                    <span class="dato-valor">${velocidad} km/h</span>
                </td>
                <td>
                    <span class="estado ${estado.clase}" style="--estado-color: ${estado.color}">
                        ${coche.event_type}
                    </span>
                </td>
            `;

            tabla.appendChild(fila);
        });

    } catch (err) {
        console.error('Error consultando la API:', err);
    }
}

actualizarSimulacion();
setInterval(actualizarSimulacion, 1000);
actualizarHistorialSNS();
setInterval(actualizarHistorialSNS, 5000);


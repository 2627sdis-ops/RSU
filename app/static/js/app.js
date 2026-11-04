const LONGITUD_CARRETERA_M = 5000.0;

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

        const coches = data.coches;
        const alertasProximidad = data.alertas_proximidad;

        const carrilIda = document.getElementById('carril-ida');
        const carrilVuelta = document.getElementById('carril-vuelta');
        const tabla = document.getElementById('tabla-coches');
        const panelAvisos = document.getElementById('panel-avisos');
        const listaAvisos = document.getElementById('lista-avisos');

        // Avisos de proximidad
        if (alertasProximidad.length > 0) {
            panelAvisos.style.display = 'block';

            listaAvisos.innerHTML = alertasProximidad.map(avisos => `
                <li>
                    <strong>${avisos.coche1}</strong> y <strong>${avisos.coche2}</strong>
                    están a <strong>${avisos.distancia} m</strong>
                    <span class="aviso-tipo">
                        ${avisos.mismo_sentido ? 'Mismo sentido' : 'Sentidos opuestos'}
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



function crearMensaje(mensaje) {
    const item = document.createElement('li');
    item.className = 'mensaje';

    const cabecera = document.createElement('div');
    cabecera.className = 'mensaje-cabecera';
    cabecera.textContent = `${mensaje.event_type} · ${mensaje.source_id}`;

    const detalle = document.createElement('p');
    detalle.textContent = mensaje.descripcion || 'Sin descripción';

    const hora = document.createElement('time');
    hora.textContent = mensaje.timestamp;

    const contenido = document.createElement('pre');
    contenido.textContent = JSON.stringify(mensaje.contenido, null, 2);

    item.append(cabecera, detalle, hora, contenido);
    return item;
}

function pintarMensajes(lista, listaId, vacioId) {
    const contenedor = document.getElementById(listaId);
    const vacio = document.getElementById(vacioId);
    contenedor.replaceChildren(...lista.map(crearMensaje));
    vacio.hidden = lista.length > 0;
}

async function actualizarDashboard() {
    try {
        const respuesta = await fetch('/api/data');
        if (!respuesta.ok) {
            throw new Error(`HTTP ${respuesta.status}`);
        }

        const datos = await respuesta.json();
        document.getElementById('titulo-rsu').textContent =
            `Monitor de ${datos.rsu_id || 'RSU'}`;
        pintarMensajes(
            datos.mensajes_vehiculos || [],
            'lista-vehiculos',
            'vacio-vehiculos'
        );
        pintarMensajes(datos.mensajes_cloud || [], 'lista-cloud', 'vacio-cloud');
    } catch (error) {
        console.error('No se pudo actualizar el dashboard de la RSU:', error);
    }
}

actualizarDashboard();
setInterval(actualizarDashboard, 1000);

from pathlib import Path


def test_ejemplo_basico():
    """Prueba unitaria básica para verificar que pytest se ejecuta."""
    assert 1 + 1 == 2


def test_archivos_del_dashboard_existen():
    """Comprueba que los recursos que necesita el servidor están en el proyecto."""
    base = Path(__file__).resolve().parent
    assert (base / "server.py").is_file()
    assert (base / "static" / "index.html").is_file()
    assert (base / "static" / "css" / "style.css").is_file()  
    assert (base / "static" / "js" / "app.js").is_file()


from mio.managers.managers import StandardStorageManager, TiledCellStorageManager, IncrementalStorageManager
from mio.managers.managers import TiledShapeStorageManager, TiledColumnStorageManager, AipsIOStorageManager

MANAGER_STORE = {
    "StandardStMan": StandardStorageManager,
    "IncrementalStMan": IncrementalStorageManager,
    "TiledCellStMan": TiledCellStorageManager,
    "TiledShapeStMan": TiledShapeStorageManager,
    "TiledColumnStMan": TiledColumnStorageManager,
    "StManAipsIO": AipsIOStorageManager
}
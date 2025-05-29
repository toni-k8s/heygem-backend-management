from datetime import datetime
import logging
import os
from cfg.setting import log_dir
log_filename = f"{datetime.now().strftime('%Y-%m-%d')}.log"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.FileHandler(os.path.join(log_dir, log_filename), encoding='utf-8'), logging.StreamHandler()])
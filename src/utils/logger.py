class Logger:
  @staticmethod
  def error(message):
    print(f"[MINI-PIPELINE-CRYPTOCURRENCE][ERROR] {message}")

  @staticmethod
  def warning(message):
    print(f"[MINI-PIPELINE-CRYPTOCURRENCE][WARNING] {message}")

  @staticmethod
  def info(message):
    print(f"[MINI-PIPELINE-CRYPTOCURRENCE][INFO] {message}")

  @staticmethod
  def debug(message):
    print(f"[MINI-PIPELINE-CRYPTOCURRENCE][DEBUG] {message}")
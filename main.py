from main_np import MainNP
from main_fh import MainFh
import warnings
import time


def main():
    warnings.filterwarnings("ignore")

    print("Initialized")

    while True:
        FH = MainFh()
        FH.fh_main()
        time.sleep(1)
        FH = None

        NP = MainNP()
        NP.np_main()
        time.sleep(1)
        NP = None


if __name__ == '__main__':
    main()

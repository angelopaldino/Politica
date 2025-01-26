import sys
import os

# Aggiungi la directory principale del progetto a sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))


from Politica.frontend.src.analisitemporale.getDays import getDays


def main():
    getDays()



if __name__ == "__main__":
    main()



# imports
import json


class Handle_json():
    _input_file = ''
    _output_file = ''
    data = []

    def __init__(self: classmethod, input_file: str, output_file: str) -> None:
        print('First step is to clean the txt log and parse to a json file with the json pattern')

        self._input_file = input_file
        self._output_file = output_file

    # ler os dados do arquivo de log como texto
    def read_file(self: classmethod) -> None:
        with open(self._input_file, 'r') as file:
            print('Reading: ', self._input_file)

            for _ in file:
                self.data.append(_.strip())
        print('finished reading')

    # Escrever dentro do arquivo de log com o padrão json
    def write_file(self: classmethod) -> None:
        with open(self._output_file, 'w') as file_json:
            print('Writing to: ', self._output_file)
            for _ in self.data:
                # Se for a primeira linha, apenas insere o começo de um array [
                if (_ == self.data[0]):
                    file_json.write('[\n')

                # se for a ultima linha, fecha o array ] e não insere ,
                elif (_ == self.data[-1]):
                    file_json.write(_)
                    file_json.write(']')

                # se não for nenhum dos dois casos, insere a linha do arquivo original e adiciona uma , no final
                else:
                    file_json.write(_)
                    file_json.write(',\n')
        print('finished writing')

# Apenas para testar o arquivo
if __name__ == '__main__':
    handle_json = Handle_json('../../data/logs.txt', '../../data/logs.json')
    handle_json.read_file()
    handle_json.write_file()

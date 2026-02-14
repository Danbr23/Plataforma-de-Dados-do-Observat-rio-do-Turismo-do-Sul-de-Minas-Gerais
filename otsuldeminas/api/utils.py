import csv
from io import StringIO
from django.http import HttpResponse

class CSVExporterResumo:
    """
    Exporta dados agregados simples (município × classificação × valor)
    """
    @staticmethod
    def export(data: dict, filename: str = "dados.csv"):
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["Município", "Classificação", "Valor"])

        # data pode estar diretamente no formato ou dentro de 'dados'
        if "dados" in data:
            data = data["dados"]

        for municipio, classificacoes in data.items():
            for classificacao, valor in classificacoes.items():
                writer.writerow([municipio, classificacao, valor])

        response = HttpResponse(buffer.getvalue(), content_type="text/csv")
        response["Content-Disposition"] = f"attachment; filename={filename}"
        return response
    
class CSVExporterTemporalSaldo:
    """
    Exporta séries temporais de saldo CAGED
    (município × classificação × mês × saldo)
    """
    @staticmethod
    def export(data: dict, filename: str = "postos_trabalho.csv"):
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["Município", "Classificação", "Ano", "Mês", "Saldo"])

        for municipio, classificacoes in data.items():
            for classificacao, anos in classificacoes.items():
                for ano, meses in anos.items():
                    
                    for registro in meses:
                        writer.writerow([
                            municipio,
                            classificacao,
                            ano,
                            registro["mes"],
                            registro["saldo"]
                        ])

        response = HttpResponse(buffer.getvalue(), content_type="text/csv")
        response["Content-Disposition"] = f"attachment; filename={filename}"
        return response
    
class CSVExporterTemporalEstoque:
    """
    Exporta séries temporais de estoque CAGED
    (município × classificação × mês × estoque)
    """
    @staticmethod
    def export(data: dict, filename: str = "postos_trabalho.csv"):
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerow(["Município", "Classificação", "Ano", "Mês", "Estoque"])

        for municipio, classificacoes in data.items():
            for classificacao, anos in classificacoes.items():
                for ano, meses in anos.items():
                    
                    for registro in meses:
                        writer.writerow([
                            municipio,
                            classificacao,
                            ano,
                            registro["mes"],
                            registro["estoque"]
                        ])

        response = HttpResponse(buffer.getvalue(), content_type="text/csv")
        response["Content-Disposition"] = f"attachment; filename={filename}"
        return response
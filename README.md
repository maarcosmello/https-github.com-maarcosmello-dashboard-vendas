# Sistema de Comissionamento (Python + Flask)

Aplicacao web para controle de vendas e comissoes com:
- Login e senha
- Perfis: administrador, vendedor e visualizador
- Cursos vinculados a empresa
- Pagamento a vista, parcelado ou recorrencia com geracao automatica de parcelas
- Dashboard com filtros e graficos por mes/ano
- Exportacao Excel com base nos filtros ativos
- Recuperacao de senha por validacao de usuario + nome completo
- Solicitação de acesso de visualizadores por curso (aprovacao admin com dias ou perpetuo)
- Titulo de cabecalho personalizado por usuario

## 1. Requisitos
- Python 3.10+
- VS Code (recomendado)

## 2. Instalacao
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Executar
```bash
python app.py
```
Acesse no navegador: `http://127.0.0.1:5000`

## 4. Primeiro acesso
1. Abra `/setup-admin`
2. Crie o administrador
3. Faça login
4. No admin, cadastre empresas e cursos
5. Comece a lancar vendas

## 5. Regras de acesso
- `admin`: gerencia tudo (usuarios, empresas, cursos, vendas e parcelas)
- `seller`: cria/edita apenas vendas que pertencem a ele
- `viewer`: visualiza dashboards e solicita acesso aos cursos para liberacao por admin

## 6. Rotas importantes
- `/dashboard`: tabela geral
- `/dashboard/recorrencia`: somente vendas de recorrencia
- `/viewer/course-access`: solicitacoes de curso para visualizadores
- `/admin/course-requests`: aprovacao/reprovacao de solicitacoes (admin)
- `/forgot-password`: recuperacao de senha

## 7. Estrutura
- `app.py`: backend Flask + regras de negocio
- `templates/`: telas HTML
- `static/`: CSS e JavaScript
- `data/commission.db`: banco SQLite

## 8. Observacoes
- Se detectar banco de modelo antigo, o app salva backup em `data/commission_legacy_*.db` e cria esquema novo.
- A exportacao usa `openpyxl`.

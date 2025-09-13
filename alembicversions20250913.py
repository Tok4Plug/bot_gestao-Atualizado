"""Full sync of buyers table with advanced tracking fields

Revision ID: 20250913_full_sync_buyers
Revises: 
Create Date: 2025-09-13 20:30:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '20250913_full_sync_buyers'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # ===========================
    # Campos principais
    # ===========================
    op.add_column('buyers', sa.Column('sid', sa.String(length=128), nullable=False, index=True))
    op.add_column('buyers', sa.Column('event_key', sa.String(length=128), nullable=False, unique=True, index=True))
    op.add_column('buyers', sa.Column('src_url', sa.Text(), nullable=True))
    op.add_column('buyers', sa.Column('cid', sa.String(length=128), nullable=True))
    op.add_column('buyers', sa.Column('gclid', sa.String(length=256), nullable=True))
    op.add_column('buyers', sa.Column('fbclid', sa.String(length=256), nullable=True))
    op.add_column('buyers', sa.Column('value', sa.Float(), nullable=True))
    op.add_column('buyers', sa.Column('source', sa.String(length=32), nullable=True, server_default='bot'))

    # ===========================
    # Dados enriquecidos / JSONB
    # ===========================
    op.add_column('buyers', sa.Column('user_data', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default='{}'))
    op.add_column('buyers', sa.Column('custom_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='{}'))

    # ===========================
    # Rastreio avançado
    # ===========================
    op.add_column('buyers', sa.Column('cookies', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('buyers', sa.Column('postal_code', sa.String(length=32), nullable=True))
    op.add_column('buyers', sa.Column('device_info', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('buyers', sa.Column('session_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True))

    # ===========================
    # Histórico / Pixels
    # ===========================
    op.add_column('buyers', sa.Column('sent_pixels', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='[]'))
    op.add_column('buyers', sa.Column('event_history', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='[]'))

    # ===========================
    # Controle de envio / tentativas
    # ===========================
    op.add_column('buyers', sa.Column('sent', sa.Boolean(), nullable=False, server_default='false', index=True))
    op.add_column('buyers', sa.Column('attempts', sa.Integer(), nullable=True, server_default='0'))
    op.add_column('buyers', sa.Column('last_attempt_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('buyers', sa.Column('last_sent_at', sa.DateTime(timezone=True), nullable=True))

    # ===========================
    # Timestamp de criação
    # ===========================
    op.add_column('buyers', sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('NOW()')))


def downgrade():
    # Remove colunas caso precise reverter
    for col in [
        'sid', 'event_key', 'src_url', 'cid', 'gclid', 'fbclid', 'value', 'source',
        'user_data', 'custom_data', 'cookies', 'postal_code', 'device_info', 'session_metadata',
        'sent_pixels', 'event_history', 'sent', 'attempts', 'last_attempt_at', 'last_sent_at',
        'created_at'
    ]:
        op.drop_column('buyers', col)
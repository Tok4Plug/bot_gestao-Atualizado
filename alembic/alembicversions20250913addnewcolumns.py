"""Add new columns to buyers table for advanced tracking

Revision ID: 20250913_add_new_columns
Revises: 
Create Date: 2025-09-13 20:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '20250913_add_new_columns'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # Adiciona colunas novas do modelo Buyer
    op.add_column('buyers', sa.Column('fbclid', sa.String(length=256), nullable=True))
    op.add_column('buyers', sa.Column('user_data', postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default='{}'))
    op.add_column('buyers', sa.Column('custom_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='{}'))
    op.add_column('buyers', sa.Column('cookies', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('buyers', sa.Column('postal_code', sa.String(length=32), nullable=True))
    op.add_column('buyers', sa.Column('device_info', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('buyers', sa.Column('session_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('buyers', sa.Column('sent_pixels', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='[]'))
    op.add_column('buyers', sa.Column('event_history', postgresql.JSONB(astext_type=sa.Text()), nullable=True, server_default='[]'))
    op.add_column('buyers', sa.Column('attempts', sa.Integer(), nullable=True, server_default='0'))
    op.add_column('buyers', sa.Column('last_attempt_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('buyers', sa.Column('last_sent_at', sa.DateTime(timezone=True), nullable=True))

def downgrade():
    # Remove colunas caso precise reverter
    op.drop_column('buyers', 'fbclid')
    op.drop_column('buyers', 'user_data')
    op.drop_column('buyers', 'custom_data')
    op.drop_column('buyers', 'cookies')
    op.drop_column('buyers', 'postal_code')
    op.drop_column('buyers', 'device_info')
    op.drop_column('buyers', 'session_metadata')
    op.drop_column('buyers', 'sent_pixels')
    op.drop_column('buyers', 'event_history')
    op.drop_column('buyers', 'attempts')
    op.drop_column('buyers', 'last_attempt_at')
    op.drop_column('buyers', 'last_sent_at')
"""

Revision ID: b77b3e4060c4
Revises: deb09119922c
Create Date: 2021-06-20 10:39:41.599873

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'b77b3e4060c4'
down_revision = 'deb09119922c'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'orders',
        sa.Column('order', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    )
    op.drop_column('orders', 'production_order_id')
    op.drop_column('orders', 'products')


def downgrade():
    op.add_column(
        'orders',
        sa.Column(
            'products',
            postgresql.JSONB(astext_type=sa.Text()),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.add_column(
        'orders',
        sa.Column(
            'production_order_id',
            sa.VARCHAR(length=256),
            autoincrement=False,
            nullable=False,
        ),
    )
    op.drop_column('orders', 'order')

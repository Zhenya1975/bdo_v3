"""sap model name

Revision ID: c6028657badb
Revises: 15753ca6d610
Create Date: 2022-06-17 08:25:31.466925

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c6028657badb'
down_revision = '15753ca6d610'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('eo_DB', schema=None) as batch_op:
        batch_op.add_column(sa.Column('sap_maker', sa.String(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('eo_DB', schema=None) as batch_op:
        batch_op.drop_column('sap_maker')

    # ### end Alembic commands ###

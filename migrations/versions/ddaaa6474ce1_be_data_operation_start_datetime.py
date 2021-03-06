"""be_data_operation_start_datetime

Revision ID: ddaaa6474ce1
Revises: 6bbe5816680f
Create Date: 2022-06-20 07:30:44.284000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ddaaa6474ce1'
down_revision = '6bbe5816680f'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('eo_DB', schema=None) as batch_op:
        batch_op.add_column(sa.Column('reported_operation_start_date', sa.DateTime(), nullable=True))

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table('eo_DB', schema=None) as batch_op:
        batch_op.drop_column('reported_operation_start_date')

    # ### end Alembic commands ###

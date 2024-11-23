import sys

from DTO import Vaccine, Logistic, Supplier, Clinic
from Repository import dbcon


output_path = ""


def initialize(input_path):
    dbcon.create_tables()

    config = open(input_path, "r")
    indexes = config.readline().split(',')
    indexes = list(map(int, indexes))

    for vaccineLine in range(indexes[0]):
        vaccineLine = config.readline().split(',')
        vaccineLine[3] = vaccineLine[3].rstrip()
        dbcon.vaccines.insert(Vaccine(*vaccineLine))

    for supplierLine in range(indexes[1]):
        supplierLine = config.readline().split(',')
        supplierLine[2] = supplierLine[2].rstrip()
        dbcon.suppliers.insert(Supplier(*supplierLine))

    for clinicLine in range(indexes[2]):
        clinicLine = config.readline().split(',')
        clinicLine[3] = clinicLine[3].rstrip()
        dbcon.clinics.insert(Clinic(*clinicLine))

    for logisticLine in range(indexes[3]):
        logisticLine = config.readline().split(',')
        logisticLine[3] = logisticLine[3].rstrip()
        dbcon.logistics.insert(Logistic(*logisticLine))


def receive_shipment(name, amount, date):
    # Takes the maximum value available for vaccine_id and increases by 1
    new_id = dbcon.vaccines.get_max_id()
    new_id = new_id[0] + 1
    # Find the supplier_id by the name we received
    curr_supplier = dbcon.suppliers.find_by_name(name)
    # Updates a new vaccine column in the data structure and increases supply amount
    dbcon.vaccines.insert(Vaccine(new_id, date, curr_supplier[0], amount))
    dbcon.logistics.increase_count_received(curr_supplier[2], amount)


def send_shipment(location, amount):
    curr_clinic = dbcon.clinics.find_by_location(location)
    dbcon.logistics.increase_count_sent(curr_clinic[3], amount)

    amount_num = int(amount)
    while amount_num > 0:
        vaccine = dbcon.vaccines.get_oldest()
        if vaccine[3] <= amount_num:
            dbcon.vaccines.remove(vaccine[0])
            amount_num -= vaccine[3]
        else:
            dbcon.vaccines.decrease_quantity(vaccine[0], amount_num)
            break

    dbcon.clinics.decrease_demand(curr_clinic[0], amount)


def execute_orders(path):
    with open(path, "r") as orders:
        for line in orders:
            line = line.split(',')
            if len(line) == 2:
                line[1] = line[1].rstrip()
                send_shipment(*line)
            if len(line) == 3:
                line[2] = line[2].rstrip()
                receive_shipment(*line)
            get_summary()


def get_summary():
    with open("output.txt", "a") as summary:
        quantity = str(*dbcon.vaccines.get_quantity_sum())
        demand = str(*dbcon.clinics.get_demand_sum())
        received = str(dbcon.logistics.get_received_sent_sum()[0])
        sent = str(dbcon.logistics.get_received_sent_sum()[1])
        summary.write(quantity + ',' + demand + ',' + received + ',' + sent + '\n')


# run using arguments command line
if __name__ == "__main__":
    output_path = sys.argv[3]
    initialize(sys.argv[1])
    execute_orders(sys.argv[2])

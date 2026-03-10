from generalresearch.models.network.rdns import dig_rdns
import faker

fake = faker.Faker()


def test_dig_rdns():
    # Actually runs dig -x. Idk how stable this is
    ip = "45.33.32.156"
    rdns_result = dig_rdns(ip)
    assert rdns_result.primary_hostname == "scanme.nmap.org"
    assert rdns_result.primary_org == "nmap"

    ip = "65.19.129.53"
    rdns_result = dig_rdns(ip)
    assert rdns_result.primary_hostname == "in1-smtp.grlengine.com"
    assert rdns_result.primary_org == "grlengine"

    ip = fake.ipv6()
    rdns_result = dig_rdns(ip)
    assert rdns_result.primary_hostname is None
    assert rdns_result.primary_org is None
    print(rdns_result.model_dump_postgres())

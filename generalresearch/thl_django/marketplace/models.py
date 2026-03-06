import uuid

from django.db import models
from django.db.models import Q


class ProbeLog(models.Model):
    """
    Table for logging probes of tasks' entry links. Typically, using playwright.
    """

    id = models.BigAutoField(primary_key=True)

    source = models.CharField(max_length=2, null=False)
    survey_id = models.CharField(max_length=32, null=False)

    # When the probe started
    started = models.DateTimeField(null=False)

    # The url that was probed
    live_url = models.CharField(max_length=3000, null=False)

    # The relative path to the har-file generated
    har_path = models.CharField(max_length=1000, null=False)

    # The result of the probe
    result = models.CharField(max_length=64, null=True)

    class Meta:
        db_table = "marketplace_probelog"

        indexes = [
            models.Index(fields=["source", "survey_id"]),
            models.Index(fields=["started"]),
        ]


"""
General naming notes:
 - Property: describes some concept about a user. e.g. age, education level, 
    the car brand they are planning on buying. 
 - Edge: an association between a user -> property -> value. The value can 
    be one of multiple different types (item, numerical, string, date). 
 - Item: represents a concept or class. I don't want to use "class" or "object" 
    b/c it conflicts with python namespaces. This is a "thing" such as 
    "male" or "honda".
 - Concept: What I'm calling something that is either a property or item, 
    such as a translation, which both properties and items have. 

Examples:
- Gender. It is valid in all countries. 
    Four possible values: male, female, non-binary, other.
    Non-binary is a subclass of other? todo
- Age. Property="age_in_years". We could also have a property "birth_date"
    value is an int (or for birthdate, a date). 
- Hispanic. Only valid in the US & CA. Options are the same in each country
    Options: Yes, No. 
    sub-options: Mexican, puerto rican, etc are subclasses of Yes.
- Education level: Valid in every country, but the options are different in 
    many countries
    - "Secondary Education" in DE, "high school" in US (not translations, 
        these are different concepts)
- postal_code: value type is a string. We could have special 
    structured/hierarchical datatypes for location regions (city, county, state).
- car's fuel source (c:fuel, l:96563, ...)

Migration Notes:
you must delete all rows from the following tables before running migration or it will fail
---mysql
delete from marketplace_externalid;
delete from marketplace_node;
delete from marketplace_property;
delete from marketplace_userprofileknowledge;
---
and we must comment out some code that writes to and reads from the UPK first before any migrations 
"""


class Property(models.Model):
    """
    Stores the list of properties and their types
    """

    id = models.UUIDField(default=uuid.uuid4, null=False, primary_key=True)
    label = models.CharField(max_length=255, null=False)
    description = models.TextField()

    # * -- zero or more
    # ? -- zero or one
    cardinality = models.CharField(max_length=9, null=False)

    TYPE_CHOICES = (
        ("n", "numerical"),
        ("x", "text"),
        ("i", "item"),
        # ('a', 'datetime'),
        # ('t', 'time'),
        # ('d', 'date'),
    )
    prop_type = models.CharField(choices=TYPE_CHOICES, max_length=1, default="c")

    class Meta:
        db_table = "marketplace_property"


class Item(models.Model):
    """
    Represent things such as male or female. A item that is unambiguously
        the same thing across countries, will have the same ID within a
        property's range, but not across different properties. For e.g.
        - "male" as a possible gender is the same thing in US & DE.
        - "high school graduate" in the US is different from "Gesamtschule"
            in Germany
        - "Honda" as an answer to "what kind of car do you drive?" is different
            than "Honda" as an answer to "what kind of car are you planning
            on buying?"
    """

    id = models.UUIDField(default=uuid.uuid4, null=False, primary_key=True)
    label = models.CharField(max_length=255, null=False)
    description = models.TextField(null=True)  # optional, for notes

    class Meta:
        db_table = "marketplace_item"


class PropertyCountry(models.Model):
    """
    This associates a property with the countries it is "allowed" to be used in.
        e.g. hispanic only applies in US & CA.

    For item properties, this is kind of unnecessary, b/c we'll know it from
        the PropertyConceptRange table. But for the others, who have no item
        ranges, we need this (like for age).
    """

    property_id = models.UUIDField(null=True)
    country_iso = models.CharField(max_length=2, null=False)

    # Used for changing how UPK is exposed. A gold standard question we've
    #   enumerated possible values (in that country) and (as best as possible)
    #   mapped them across marketplaces. A property not marked as gold-standard
    #   maybe has 1) marketplace qid associations & 2) category associations,
    #   but doesn't have a defined "range" (list of allowed items in a
    #   multiple choice question). Used for exposing a user's profiling data &
    #   for the Nudge API.
    gold_standard = models.BooleanField(default=False)

    class Meta:
        db_table = "marketplace_propertycountry"

        indexes = [
            models.Index(fields=["property_id"]),
            models.Index(fields=["country_iso"]),
        ]


class PropertyItemRange(models.Model):
    """
    "Range" means the set of possible values for this property in this country
    e.g. (gender (every country): male, female, other),
    or education (us): high school, university, whatever, which is different
        than the options in germany.
    """

    property_id = models.UUIDField(null=True)
    item = models.ForeignKey(Item, on_delete=models.CASCADE)
    country_iso = models.CharField(max_length=2, null=False)

    class Meta:
        db_table = "marketplace_propertyitemrange"
        indexes = [models.Index(fields=["country_iso", "property_id"])]


class ConceptTranslation(models.Model):
    """
    One table for both properties and classes.
    """

    concept_id = models.UUIDField()
    language_iso = models.CharField(max_length=3)
    text = models.TextField()

    class Meta:
        db_table = "marketplace_concepttranslation"

        indexes = [
            models.Index(fields=["concept_id"]),
            models.Index(fields=["language_iso"]),
        ]


class PropertyMarketplaceAssociation(models.Model):
    """
    Associates a property with a marketplace's question ID (many-to-many)
    """

    property_id = models.UUIDField(null=True)
    source = models.CharField(max_length=1, null=False)
    question_id = models.CharField(max_length=32, null=False)

    class Meta:
        db_table = "marketplace_propertymarketplaceassociation"

        indexes = [
            models.Index(fields=["source", "question_id"]),
            models.Index(fields=["property_id"]),
        ]


class Category(models.Model):
    """
    https://cloud.google.com/natural-language/docs/categories
    https://developers.google.com/adwords/api/docs/appendix/verticals
    https://developers.google.com/adwords/api/docs/appendix/codes-formats
    """

    id = models.AutoField(primary_key=True)
    uuid = models.UUIDField(unique=True)
    parent = models.ForeignKey("Category", null=True, on_delete=models.SET_NULL)
    adwords_vertical_id = models.CharField(max_length=8, null=True)
    label = models.CharField(max_length=255)
    # stores a "path-style" label for easy searching, tagging, convenience.
    #   e.g. '/Hobbies & Leisure/Outdoors/Fishing'
    path = models.CharField(max_length=1024, null=True)

    class Meta:
        db_table = "marketplace_category"


class PropertyCategoryAssociation(models.Model):
    """
    Associates a property with a category (many-to-many)
    """

    property_id = models.UUIDField(null=True)
    category = models.ForeignKey(Category, on_delete=models.CASCADE)

    class Meta:
        db_table = "marketplace_propertycategoryassociation"

        indexes = [models.Index(fields=["property_id"])]


# class ExternalID(models.Model):
#     """"
#     Probably mostly for location based concepts (geocode ID for "Florida",
#       or wikidata ID for whatever).
#     """
#     concept_id = models.UUIDField(null=False)
#     curie = models.CharField(max_length=255, null=False)
#
# class PropertyAnnotation(models.Model):
#     """
#       This could be used to define a range for non item-properties
#       (e.g. age), maybe... or maybe associating properties with another
#       (age_in_years <-> birth_date)
#     """
#     pass
#
# class ClassStatement(models.Model):
#     """This could be used to associate classes between one another across
#           questions. For example to link the two Honda concepts in the
#           above example.
#     """
#     pass


class UserProfileKnowledge(models.Model):
    """
    This only stores the most recent knowledge per user_id/property_id/question
    Purposely not using foreign keys for the property or values b/c this table is
        going to be huge and this will add complexity, overhead, unintended consequences...
    """

    user_id = models.PositiveIntegerField()
    property_id = models.UUIDField()
    # value = models.UUIDField()

    # If we ask the user, we'll have a session and question ID. We could also
    #   accept a 'gender' from a BP, and we may not know exactly how it was
    #   asked, so we have to support no question_id.
    session_id = models.UUIDField(null=True)
    question_id = models.UUIDField(null=True)

    # If question_id is optional, we need the locale here also, even though it
    #   would be inferable from the question_id which itself is locale-scoped.
    country_iso = models.CharField(max_length=2, default="us")

    # I don't think lang should be a field here. If we ask the question, we'll
    #   know from the question_id the lang. Otherwise, we may not know what
    #   lang the question was asked in. The way we have the itemrange set up,
    #   the lang does not affect the possible options, only the translation.
    #   I think it is a really, really small edge case in which the language
    #   would affect an answer here.
    # language_iso = models.CharField(max_length=3, default='eng')

    # when this specific edge (user, prop, value) was created/updated/added/changed
    created = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UserProfileKnowledgeItem(UserProfileKnowledge):
    """
    Same as UserProfileKnowledge but for value type of numerical
    """

    value = models.UUIDField(null=False)

    class Meta:
        db_table = "marketplace_userprofileknowledgeitem"
        indexes = [
            models.Index(fields=["user_id"]),
            models.Index(fields=["created"]),
            models.Index(fields=["property_id"]),
        ]


class UserProfileKnowledgeNumerical(UserProfileKnowledge):
    """
    Same as UserProfileKnowledge but for value type of numerical
    """

    value = models.FloatField(null=False)

    class Meta:
        db_table = "marketplace_userprofileknowledgenumerical"
        indexes = [
            models.Index(fields=["user_id"]),
            models.Index(fields=["created"]),
            models.Index(fields=["property_id"]),
        ]


class UserProfileKnowledgeText(UserProfileKnowledge):
    """
    Same as UserProfileKnowledge but for value type of numerical
    """

    value = models.CharField(max_length=1024)

    class Meta:
        db_table = "marketplace_userprofileknowledgetext"
        indexes = [
            models.Index(fields=["user_id"]),
            models.Index(fields=["created"]),
            models.Index(fields=["property_id"]),
        ]


class Question(models.Model):
    """
    Stores the info about a Question that is asked to a user.
    """

    id = models.UUIDField(default=uuid.uuid4, primary_key=True, null=False)

    # Used for detecting changes to marketplace questions (that were changed
    #   by the marketplace)
    md5sum = models.CharField(max_length=32, null=True)
    country_iso = models.CharField(max_length=2, default="us")
    language_iso = models.CharField(max_length=3, default="eng")

    # this is either a upk code (e.g. gr:gender) or a marketplace question ID
    #   (e.g. c:gender (cint's gender question))
    property_code = models.CharField(max_length=64)

    # conforms to question.schema.json (lives in thl-yieldman/mrpq/jsonschema)
    data = models.JSONField(default=dict)

    # shortcut for determining if the data.task_score > 0
    is_live = models.BooleanField(default=False)

    # Optionally describes custom, manual or automatic, modifications made to
    #   a question. for e.g.: marking it as never to be asked, or adding a
    #   "None of the above" option
    custom = models.JSONField(default=dict)

    # When this question was last modified
    last_updated = models.DateTimeField(null=True)

    # Human-readable template for explaining how a user's answer to this
    #   question affects eligibility
    explanation_template = models.TextField(max_length=255, null=True)

    # A very short, natural-language explanation fragment that can be combined
    #   with others into a single sentence
    explanation_fragment_template = models.TextField(max_length=255, null=True)

    class Meta:
        db_table = "marketplace_question"

        indexes = [
            models.Index(fields=["last_updated"]),
            models.Index(fields=["property_code"]),
        ]


class UserQuestionAnswer(models.Model):
    """
    Stores the info about the event of a user answering a question.
    This is distinct from UPK b/c, for e.g. a user could be asked
     a) what is your gender? (male, female) or
     b) what is your gender? (male, female, other)
    The UPK table would store the user's latest "gr:gender" -> answer
    The user question answer would store the info about which question they were asked,
        and what they answered.
    """

    question = models.ForeignKey("Question", on_delete=models.DO_NOTHING)
    created = models.DateTimeField()
    session_id = models.UUIDField(null=True)
    user_id = models.IntegerField()

    # The user's answer to the question. Stores the actual value for text
    #   questions, or the selected choice's codes for MC. Always a list!!
    # e.g. ["92116"] for a text entry, or ["3", "7", "9"] for multiple choice
    answer = models.JSONField(default=list)

    # We'll save in here the marketplace answers that we've inferred/generated
    #   from the answer e.g. {"l:123": ["1", "2"], "c:643": ["6", "5"]}
    calc_answer = models.JSONField(default=dict)

    class Meta:
        db_table = "marketplace_userquestionanswer"

        indexes = [
            models.Index(fields=["user_id", "question_id", "-created"]),
            models.Index(fields=["created"]),
        ]


class UserGroup(models.Model):
    # Used for tracking user-user identity
    # If userA == userB and userB == userC, then userA == userC (transitive)
    user_id = models.PositiveIntegerField()
    user_group = models.UUIDField()
    created = models.DateTimeField(null=False)

    class Meta:
        db_table = "marketplace_usergroup"

        unique_together = ("user_id", "user_group")
        indexes = [
            models.Index(fields=["created"]),
            models.Index(fields=["user_id"]),
            models.Index(fields=["user_group"]),
        ]


class Buyer(models.Model):
    id = models.BigAutoField(primary_key=True)

    # The marketplace's 2-letter code {l, c, d, h, s, ...}
    source = models.CharField(max_length=2, null=False)

    # The marketplace's ID/code for this buyer
    code = models.CharField(max_length=128, null=False)

    # optional text name for the buyer, if available
    label = models.CharField(max_length=255, null=True)

    # when this entry was made, or when the buyer was first seen
    created = models.DateTimeField(auto_now_add=True, null=False)

    class Meta:
        db_table = "marketplace_buyer"

        unique_together = ("source", "code")
        indexes = [models.Index(fields=["created"])]


class BuyerGroup(models.Model):
    """
    If we know that a buyer is the same buyer across different marketplaces,
        we can link them here.

    Constraints here enforce:
     - a buyer can only be in 1 group, once (no duplicates)
     - a group can have multiple buyers
    """

    id = models.BigAutoField(primary_key=True)

    # This is the buyer group's universal ID (can expose this)
    group = models.UUIDField(default=uuid.uuid4, null=False)

    # OneToOneField: Same thing as a ForeignKey with unique = True
    buyer = models.OneToOneField(Buyer, on_delete=models.RESTRICT)
    created = models.DateTimeField(auto_now_add=True, null=False)

    class Meta:
        db_table = "marketplace_buyergroup"

        indexes = [
            models.Index(fields=["created"]),
            models.Index(fields=["group"]),
        ]


class Survey(models.Model):
    id = models.BigAutoField(primary_key=True)
    # The "unique" key in this table is:
    #   (source, survey_id)
    source = models.CharField(max_length=2, null=False)
    survey_id = models.CharField(max_length=32, null=False)
    buyer = models.ForeignKey(Buyer, null=True, on_delete=models.PROTECT)
    created_at = models.DateTimeField(auto_now_add=True, null=False)
    updated_at = models.DateTimeField(auto_now=True, null=False)

    # This I'm not sure about. We want data to be able to return in an
    #   offerwall "why" a user is eligible for this survey. The complexity
    #   is mapping the mp-specific question codes to the question ids we
    #   ask, and unsure where best to do that.
    #
    # Also, 99% are going to have age/gender, which is a waste of data, so
    #   maybe we want to structure this differently.
    # # used_question_ids = models.JSONField(default=list)
    # Going with this instead. We'll have to structure it clearly in pydantic:
    eligibility_criteria = models.JSONField(null=True)

    is_live = models.BooleanField(null=False)
    is_recontact = models.BooleanField(default=False)

    # .... more metadata: survey platform/host, category, etc ...

    class Meta:
        db_table = "marketplace_survey"
        indexes = [
            models.Index(fields=["source", "is_live"]),
            # Tiny index compared to ----^, but only if we filter WHERE is_live = TRUE
            models.Index(
                fields=["source"],
                name="survey_live_by_source",
                condition=models.Q(is_live=True),
            ),
            models.Index(fields=["created_at"]),
            models.Index(fields=["updated_at"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["source", "survey_id"],
                name="uniq_survey_source_survey_id",
            )
        ]


class SurveyCategory(models.Model):
    """
    Associates a Survey with one or more Categories, with an optional strength / weight.
    """

    id = models.BigAutoField(primary_key=True)
    survey = models.ForeignKey(Survey, on_delete=models.CASCADE)
    category = models.ForeignKey(Category, on_delete=models.RESTRICT)

    # Strength / confidence / relevance. 0.0–1.0 probability
    # The sum(strength) for a survey should add up to 1.
    strength = models.FloatField(
        null=True,
        help_text="Relative relevance or confidence (0–1)",
    )

    class Meta:
        db_table = "marketplace_surveycategory"
        constraints = [
            models.UniqueConstraint(
                fields=["survey", "category"],
                name="uniq_survey_category",
            )
        ]


class SurveyStat(models.Model):
    id = models.BigAutoField(primary_key=True)

    # The "unique" key in this table (what all stats are calculated for) is:
    #   ((source, survey_id)=survey, quota_id, country_iso, version)
    survey = models.ForeignKey(Survey, on_delete=models.RESTRICT)

    # We could calculate stats for a specific quota. This should be nullable,
    # but that is problematic in a unique key b/c NULL != NULL, so the comparison
    # doesn't use the index properly. Instead of null, use a sentinel value
    # like "__all__".
    quota_id = models.CharField(max_length=32, null=False)

    # We could also have stats per country, if a survey is open to multiple countries.
    # Use 'ZZ' if a survey is open to any country, and we calculate the stats pooled.
    country_iso = models.CharField(max_length=2, null=False)

    cpi = models.DecimalField(max_digits=8, decimal_places=5, null=False)
    complete_too_fast_cutoff = models.IntegerField(help_text="Seconds")

    # ---- Distributions ----

    prescreen_conv_alpha = models.FloatField()
    prescreen_conv_beta = models.FloatField()

    conv_alpha = models.FloatField()
    conv_beta = models.FloatField()

    dropoff_alpha = models.FloatField()
    dropoff_beta = models.FloatField()

    completion_time_mu = models.FloatField()
    completion_time_sigma = models.FloatField()

    # Eligibility modeled probabilistically
    mobile_eligible_alpha = models.FloatField()
    mobile_eligible_beta = models.FloatField()

    desktop_eligible_alpha = models.FloatField()
    desktop_eligible_beta = models.FloatField()

    tablet_eligible_alpha = models.FloatField()
    tablet_eligible_beta = models.FloatField()

    # ---- Scalar risk / quality metrics ----

    long_fail_rate = models.FloatField()
    user_report_coeff = models.FloatField()
    recon_likelihood = models.FloatField()

    # Survey penalty gets converted to score_x0 = 0 and score_x1 = (1-penalty)
    #   (these are the coefficients that'll be applied to the final score, e.g.
    #   score_x0 + {x}*score_x1 + {x}*score_x2^2 ... )
    score_x0 = models.FloatField()
    score_x1 = models.FloatField()

    # generalized/predicated score
    score = models.FloatField()

    # ---- Metadata ----
    # We can use this to compare yield-management strategies, or A/B test stats, etc...
    version = models.PositiveIntegerField(help_text="Bump when logic changes")

    # Set when a row is created, updated, or turned not live (even if stats didn't change)
    updated_at = models.DateTimeField(auto_now=True)

    # These are de-normalized from the Survey for ease of join / SQL operations.
    #   They should match exactly the fields on the referenced Survey.
    survey_is_live = models.BooleanField(null=False)
    survey_survey_id = models.CharField(max_length=32, null=False)
    survey_source = models.CharField(max_length=2, null=False)

    class Meta:
        db_table = "marketplace_surveystat"
        indexes = [
            models.Index(
                fields=["survey"],
                name="surveystat_live_survey_idx",
                condition=Q(survey_is_live=True),
            ),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=[
                    "survey",
                    "quota_id",
                    "country_iso",
                    "version",
                ],
                name="uniq_surveystat_survey_quota_country_version",
            )
        ]


# class SurveyStatBP(models.Model):
#     """
#     Defines Brokerage Product-specific adjustments
#         for a survey.
#
#     Notes:
#     - For the survey as a whole, no quota, country_iso, version.
#     - This is currently only used for rate-limiting completes from
#         a BP into a particular survey.
#     - This table is "sparse" in that 99% of live surveys
#         won't have any bp-specific adjustments.
#     """
#
#     id = models.BigAutoField(primary_key=True)
#
#     product_id = models.UUIDField(null=False)
#
#     survey = models.ForeignKey(
#         Survey,
#         on_delete=models.CASCADE,
#     )
#
#     # Survey penalty gets converted to score_x0 = 0 and score_x1 = (1-penalty)
#     #   (these are the coefficients that'll be applied to the final score, e.g.
#     #   score_x0 + {x}*score_x1 + {x}*score_x2^2 ... )
#     score_x0 = models.FloatField()
#     score_x1 = models.FloatField()


#
# class SurveyStatusBucket(models.Model):
#     """
#     Aggregated counts of wall.status
#     Grouped by:
#       ((source, survey_id)=survey, quota_id, country_iso, product_id, bucket_start, bucket_size, status)
#     """
#
#     id = models.BigAutoField(primary_key=True)
#
#     survey = models.ForeignKey(Survey, on_delete=models.RESTRICT)
#
#     quota_id = models.CharField(max_length=32, null=True)
#     country_iso = models.CharField(max_length=2, null=False)
#
#     product_id = models.UUIDField(null=False)
#
#     status = models.CharField(max_length=1, null=True)
#
#     count = models.PositiveIntegerField(null=False, default=0)
#
#     bucket_start = models.DateTimeField(
#         help_text="UTC start of aggregation bucket"
#     )
#
#     BUCKET_SIZES = (
#         (3600, "hour"),
#         (86400, "day"),
#     )
#
#     bucket_size = models.PositiveSmallIntegerField(
#         help_text="Bucket size in seconds (e.g. 3600, 86400)",
#         choices=BUCKET_SIZES,
#     )
#
#     updated_at = models.DateTimeField(auto_now=True)
#
#     class Meta:
#         db_table = "marketplace_surveystatusbucket"
#         constraints = [
#             models.UniqueConstraint(
#                 fields=[
#                     "survey",
#                     "quota_id",
#                     "country_iso",
#                     "product_id",
#                     "status",
#                     "bucket_start",
#                     "bucket_size",
#                 ],
#                 nulls_distinct=False,
#                 name="uniq_surveystatuscount",
#             )
#         ]
#         indexes = [
#             models.Index(fields=["survey", "bucket_start"]),
#             models.Index(fields=["status", "bucket_start"]),
#             models.Index(fields=["product_id", "bucket_start"]),
#         ]

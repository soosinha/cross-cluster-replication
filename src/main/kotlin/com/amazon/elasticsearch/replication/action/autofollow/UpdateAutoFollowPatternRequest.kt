/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.elasticsearch.replication.action.autofollow

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ObjectParser
import org.elasticsearch.common.xcontent.XContentParser
import com.amazon.elasticsearch.replication.action.index.ReplicateIndexRequest
import java.util.function.BiConsumer

class UpdateAutoFollowPatternRequest: AcknowledgedRequest<UpdateAutoFollowPatternRequest> {

    lateinit var connection: String
    lateinit var patternName: String
    var pattern: String? = null
    var assumeRoles: HashMap<String, String>? = null // roles to assume - {leader_fgac_role: role1, follower_fgac_role: role2}

    enum class Action {
        ADD, REMOVE
    }
    lateinit var action : Action

    private constructor()

    companion object {
        private val AUTOFOLLOW_REQ_PARSER = ObjectParser<UpdateAutoFollowPatternRequest, Void>("AutoFollowRequestParser") { UpdateAutoFollowPatternRequest() }
        init {
            AUTOFOLLOW_REQ_PARSER.declareString(UpdateAutoFollowPatternRequest::connection::set, ParseField("connection"))
            AUTOFOLLOW_REQ_PARSER.declareString(UpdateAutoFollowPatternRequest::patternName::set, ParseField("name"))
            AUTOFOLLOW_REQ_PARSER.declareString(UpdateAutoFollowPatternRequest::pattern::set, ParseField("pattern"))

            AUTOFOLLOW_REQ_PARSER.declareObjectOrDefault(BiConsumer { reqParser: UpdateAutoFollowPatternRequest,
                                                                      roles: HashMap<String, String> -> reqParser.assumeRoles = roles},
                    ReplicateIndexRequest.FGAC_ROLES_PARSER, null, ParseField("assume_roles"))
        }
        fun fromXContent(xcp: XContentParser, action: Action) : UpdateAutoFollowPatternRequest {
            val updateAutofollowReq = AUTOFOLLOW_REQ_PARSER.parse(xcp, null)
            updateAutofollowReq.action = action
            if(updateAutofollowReq.assumeRoles?.size == 0) {
                updateAutofollowReq.assumeRoles = null
            }
            return updateAutofollowReq
        }
    }


    constructor(connection: String, patternName: String, pattern: String?, action: Action) {
        this.connection = connection
        this.patternName = patternName
        this.pattern = pattern
        this.action = action
    }

    constructor(inp: StreamInput) : super(inp) {
        connection = inp.readString()
        patternName = inp.readString()
        pattern = inp.readOptionalString()
        action = inp.readEnum(Action::class.java)
    }


    override fun validate(): ActionRequestValidationException? {
        var validationException = ActionRequestValidationException()
        if(!this::connection.isInitialized ||
                !this::patternName.isInitialized) {
            validationException.addValidationError("Missing connection or name in the request")
        }

        if(assumeRoles != null && (assumeRoles!!.size < 2 || assumeRoles!![ReplicateIndexRequest.LEADER_FGAC_ROLE] == null ||
                        assumeRoles!![ReplicateIndexRequest.FOLLOWER_FGAC_ROLE] == null)) {
            validationException.addValidationError("Need roles for ${ReplicateIndexRequest.LEADER_FGAC_ROLE} and " +
                    "${ReplicateIndexRequest.FOLLOWER_FGAC_ROLE}")
        }

        if(action == Action.REMOVE) {
            if(pattern != null) {
                validationException.addValidationError("Unexpected pattern")
            }
        } else if(pattern == null) {
            validationException.addValidationError("Missing pattern")
        }

        return if(validationException.validationErrors().isEmpty()) return null else validationException
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(connection)
        out.writeString(patternName)
        out.writeOptionalString(pattern)
        out.writeEnum(action)
    }
}